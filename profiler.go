package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apex/log"
	"github.com/finkf/gofiler"
	"github.com/finkf/pcwgo/api"
	"github.com/finkf/pcwgo/db"
	"github.com/finkf/pcwgo/service"
)

type profiler struct {
	project *db.Project
	config  gofiler.LanguageConfiguration
	profile api.Profile
	types   map[string]int
}

func (p profiler) BookID() int {
	return p.project.BookID
}

func (profiler) Name() string {
	return "profiler"
}

func (p *profiler) Run(ctx context.Context) error {
	log.Debug("profiler: run()")
	if err := p.findLanguage(); err != nil {
		return fmt.Errorf("cannot profile: %v", err)
	}
	if err := p.runProfiler(ctx); err != nil {
		return fmt.Errorf("cannot profile: %v", err)
	}
	if err := p.writeProfile(); err != nil {
		return fmt.Errorf("cannot profile: %v", err)
	}
	if err := p.insertProfileIntoDB(); err != nil {
		return fmt.Errorf("cannot profile: %v", err)
	}
	return nil
}

func (p *profiler) findLanguage() error {
	log.Debug("profiler: findLanguage()")
	config, err := gofiler.FindLanguage(languageDir, p.project.Lang)
	if err != nil {
		// if err == gofiler.ErrorLanguageNotFound could be handled
		// like http.StatusNotFound but we ignore this for now.
		return fmt.Errorf("cannot find language %s: %v", p.project.Lang, err)
	}
	log.Debugf("profiler: found language: %s: %s", config.Language, config.Path)
	p.config = config
	return nil
}

func (p *profiler) runProfiler(ctx context.Context) error {
	log.Debug("profiler: runProfiler()")
	var tokens []gofiler.Token
	err := eachLine(p.project.BookID, func(line db.Chars) error {
		return eachWord(line, func(word db.Chars) error {
			tokens = append(tokens, gofiler.Token{OCR: word.OCR()})
			if word.IsFullyCorrected() {
				tokens[len(tokens)-1].COR = word.Cor()
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("cannot run profiler: %v", err)
	}
	log.Debugf("profiler: profiled %d tokens", len(tokens))
	profile, err := gofiler.Run(ctx, profbin, p.config.Path, tokens, logger{})
	if err != nil {
		return fmt.Errorf("cannot run profiler: %v", err)
	}
	p.profile = api.Profile{Profile: profile, BookID: p.project.BookID}
	return nil
}

func (p *profiler) writeProfile() (err error) {
	log.Debug("profiler: writeProfile()")
	dest := filepath.Join(projectDir, p.project.Directory, "profile.json.gz")
	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("cannot write profile %s: %v", dest, err)
	}
	defer func() {
		if xerr := out.Close(); xerr != nil && err == nil {
			err = fmt.Errorf("cannot write profile %s: %v", dest, xerr)
		}
	}()
	gzip := gzip.NewWriter(out)
	defer func() {
		if xerr := gzip.Close(); xerr != nil && err == nil {
			err = fmt.Errorf("cannot write profile %s: %v", dest, xerr)
		}
	}()
	if err := json.NewEncoder(gzip).Encode(p.profile); err != nil {
		return fmt.Errorf("cannot write profile %s: %v", dest, err)
	}
	return nil
}

func (p *profiler) insertProfileIntoDB() error {
	log.Debug("profiler: insertProfileIntoDB()")
	t := db.NewTransaction(service.Pool().Begin())
	p.types = make(map[string]int)
	t.Do(func(dtb db.DB) error {
		tables := []string{"suggestions", "errorpatterns", "typcounts"}
		for _, table := range tables {
			stmnt := fmt.Sprintf("DELETE FROM %s WHERE bookid=?", table)
			_, err := db.Exec(service.Pool(), stmnt, p.profile.BookID)
			if err != nil {
				return fmt.Errorf("cannot insert profile into database: %v", err)
			}
		}
		for _, interp := range p.profile.Profile {
			if len(interp.Candidates) == 0 {
				continue
			}
			tid, err := p.insertInterpretation(dtb, interp)
			if err != nil {
				return fmt.Errorf("cannot insert profile: %v", err)
			}
			for i, cand := range interp.Candidates {
				// skip if weight is too low
				if float64(cand.Weight) <= cutoff {
					continue
				}
				if err := p.insertCandidate(dtb, cand, tid, i == 0); err != nil {
					return fmt.Errorf(
						"cannot insert profile into database: %v", err)
				}
			}
		}
		stmnt := "UPDATE books SET statusid=? WHERE bookid=? AND statusid<?"
		st := db.StatusIDProfiled
		if _, err := db.Exec(dtb, stmnt, st, p.profile.BookID, st); err != nil {
			return fmt.Errorf("cannot insert profile into database: %v", err)
		}
		return nil
	})
	return t.Done()
}

func (p *profiler) insertInterpretation(
	dtb db.DB, interp gofiler.Interpretation,
) (int, error) {
	tid, err := db.NewType(dtb, interp.OCR, p.types)
	if err != nil {
		return 0, fmt.Errorf("cannot insert interpretation: %v", err)
	}
	const stmnt = "INSERT INTO typcounts " +
		"(typid,bookid,counts) " +
		"VALUES (?,?,?) " +
		"ON DUPLICATE KEY UPDATE counts = counts + ?"
	_, err = db.Exec(dtb, stmnt, tid, p.project.BookID, interp.N, interp.N)
	if err != nil {
		return 0, fmt.Errorf("cannot insert interpretations: %v", err)
	}
	return tid, nil
}

func (p *profiler) insertCandidate(
	dtb db.DB, cand gofiler.Candidate, tid int, top bool,
) error {
	stmt := fmt.Sprintf("INSERT INTO %s (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "+
		"VALUES(?,?,?,?,?,?,?,?,?,?)",
		db.SuggestionsTableName,
		db.SuggestionsTableBookID,
		db.SuggestionsTableTokenTypeID,
		db.SuggestionsTableSuggestionTypeID,
		db.SuggestionsTableModernTypeID,
		db.SuggestionsTableDict,
		db.SuggestionsTableWeight,
		db.SuggestionsTableDistance,
		db.SuggestionsTableTopSuggestion,
		db.SuggestionsTableHistPatterns,
		db.SuggestionsTableOCRPatterns)
	sid, err := db.NewType(dtb, cand.Suggestion, nil)
	if err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	mid, err := db.NewType(dtb, cand.Modern, p.types)
	if err != nil {
		return fmt.Errorf("cannot insert candiate: %v", err)
	}
	res, err := db.Exec(dtb, stmt, p.project.BookID, tid, sid, mid,
		cand.Dict, cand.Weight, cand.Distance, top,
		patternString(cand.HistPatterns),
		patternString(cand.OCRPatterns))
	if err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	nid, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}

	if err := p.insertPatterns(dtb, cand.HistPatterns, int(nid), false); err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	if err := p.insertPatterns(dtb, cand.OCRPatterns, int(nid), true); err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	return nil
}

func (p *profiler) insertPatterns(
	dtb db.DB, ps []gofiler.Pattern, sid int, ocr bool) error {
	const stmt = "INSERT INTO errorpatterns " +
		"(suggestionID,bookID,pattern,ocr) " +
		"VALUES (?,?,?,?)"
	for _, pat := range ps {
		if _, err := db.Exec(dtb, stmt, sid, p.project.BookID, pat.Left+":"+pat.Right, ocr); err != nil {
			return fmt.Errorf("cannot insert pattern: %v", err)
		}
	}
	return nil
}

func patternString(ps []gofiler.Pattern) string {
	var strs []string
	for _, p := range ps {
		strs = append(strs, fmt.Sprintf("%s:%s:%d", p.Left, p.Right, p.Pos))
	}
	return strings.Join(strs, ",")
}

func selectBookLines(bookID int) ([]db.Chars, error) {
	const stmt = "SELECT Cor,OCR,LineID FROM " + db.ContentsTableName +
		" WHERE BookID=? ORDER BY PageID, LineID, Seq"
	rows, err := db.Query(service.Pool(), stmt, bookID)
	if err != nil {
		return nil, fmt.Errorf("cannot select lines for book ID %d: %v",
			bookID, err)
	}
	defer rows.Close()
	lineID := -1
	var lines []db.Chars
	for rows.Next() {
		var tmp int
		var char db.Char
		if err := rows.Scan(&char.Cor, &char.OCR, &tmp); err != nil {
			return nil, fmt.Errorf("cannot select lines for book ID %d: %v",
				bookID, err)
		}
		if tmp != lineID {
			lines = append(lines, nil)
			lineID = tmp
		}
		lines[len(lines)-1] = append(lines[len(lines)-1], char)
	}
	return lines, nil
}

func eachLine(bookID int, f func(db.Chars) error) error {
	lines, err := selectBookLines(bookID)
	if err != nil {
		return fmt.Errorf("cannot load lines for book ID %d: %v",
			bookID, err)
	}
	for _, line := range lines {
		if err := f(line); err != nil {
			return err
		}
	}
	return nil
}

func eachWord(line db.Chars, f func(db.Chars) error) error {
	for t, r := line.NextWord(); len(t) > 0; t, r = r.NextWord() {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}

type logger struct{}

func (logger) Log(msg string) {
	log.Debug(msg)
}
