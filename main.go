package main // import "github.com/finkf/pcwprofiler"

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/finkf/gofiler"
	"github.com/finkf/pcwgo/api"
	"github.com/finkf/pcwgo/db"
	"github.com/finkf/pcwgo/jobs"
	"github.com/finkf/pcwgo/service"
	log "github.com/sirupsen/logrus"
)

var (
	listen      = ":80"
	projectDir  = "/project-data"
	languageDir = "/language-data"
	profbin     = "/apps/profiler"
	dsn         = ""
	debug       = false
	cutoff      = 1e-4
)

func init() {
	flag.StringVar(&listen, "listen", listen, "set host")
	flag.StringVar(&projectDir, "project-dir",
		projectDir, "set project base dir")
	flag.StringVar(&languageDir, "language-dir",
		languageDir, "set profiler's language backend")
	flag.StringVar(&profbin, "profiler",
		profbin, "path to profiler executable")
	flag.StringVar(&dsn, "dsn", dsn,
		"set mysql connection DSN (user:pass@proto(host)/dbname)")
	flag.Float64Var(&cutoff, "cutoff",
		cutoff, "set cutoff weight for profiler suggestions")
	flag.BoolVar(&debug, "debug", debug, "enable debugging")
}

func main() {
	// command line args
	flag.Parse()
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	// init
	if err := service.Init(dsn); err != nil {
		log.Fatalf("cannot initialize service: %v", err)
	}
	defer service.Close()
	// start jobs
	jobs.Init(service.Pool())
	defer jobs.Close()
	// start server
	http.HandleFunc("/profile/languages",
		service.WithLog(service.WithMethods(http.MethodGet, getLanguages())))
	http.HandleFunc("/profile/books/",
		service.WithLog(service.WithMethods(
			http.MethodGet, service.WithProject(getProfile()),
			http.MethodPost, service.WithProject(run()))))
	http.HandleFunc("/profile/patterns/books/",
		service.WithLog(service.WithMethods(
			http.MethodGet, service.WithProject(getPatterns()))))
	http.HandleFunc("/profile/suspicious/books/",
		service.WithLog(service.WithMethods(
			http.MethodGet, service.WithProject(getSuspiciousWords()))))
	http.HandleFunc("/profile/adaptive/books/",
		service.WithLog(service.WithMethods(
			http.MethodGet, service.WithProject(getAdaptiveTokens()))))
	log.Infof("listening on %s", listen)
	log.Fatal(http.ListenAndServe(listen, nil))
}

func getLanguages() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		configs, err := gofiler.ListLanguages(languageDir)
		if err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot list languages: %v", err)
			return
		}
		ls := api.Languages{Languages: make([]string, len(configs))}
		for i := range configs {
			ls.Languages[i] = configs[i].Language
		}
		service.JSONResponse(w, ls)
	}
}

func getProfile() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		q := r.URL.Query()["q"]
		if len(q) == 0 { // return the whole profile
			getWholeProfile(w, r, d.Project)
			return
		}
		queryProfile(w, d.Project, q)
	}
}

func getWholeProfile(w http.ResponseWriter, r *http.Request, p *db.Project) {
	w.Header().Add("Content-Encoding", "gzip")
	w.Header().Add("Content-Type", "application/json")
	http.ServeFile(w, r, filepath.Join(projectDir, p.Directory, "profile.json.gz"))
}

func queryProfile(w http.ResponseWriter, p *db.Project, qs []string) {
	ss := api.Suggestions{
		BookID:      p.BookID,
		Suggestions: make(map[string][]api.Suggestion),
	}
	for _, q := range qs {
		if err := selectSuggestions(q, &ss); err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot get suggestions: %v", err)
			return
		}
	}
	service.JSONResponse(w, ss)
}

func selectSuggestions(q string, ss *api.Suggestions) error {
	const stmt = "SELECT s.id,s.weight,s.distance,s.dict," +
		"s.histpatterns,s.ocrpatterns," +
		"s.topsuggestion,t1.typ,t2.typ,t3.typ " +
		"FROM suggestions s " +
		"JOIN types t1 ON s.tokentypid=t1.id " +
		"JOIN types t2 ON s.suggestiontypid=t2.id " +
		"JOIN types t3 ON s.moderntypid=t3.id " +
		"WHERE s.bookid=? AND t1.typ=?"
	rows, err := db.Query(service.Pool(), stmt, ss.BookID, strings.ToLower(q))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var h, o string
		var s api.Suggestion
		if err := rows.Scan(&s.ID, &s.Weight, &s.Distance, &s.Dict, &h, &o,
			&s.Top, &s.Token, &s.Suggestion, &s.Modern); err != nil {
			return err
		}
		s.HistPatterns = strings.Split(h, ",")
		s.OCRPatterns = strings.Split(o, ",")
		s.Token = applyCasing(q, s.Token)
		s.Suggestion = applyCasing(q, s.Suggestion)
		s.Modern = applyCasing(q, s.Modern)
		ss.Suggestions[q] = append(ss.Suggestions[q], s)
	}
	return nil
}

func applyCasing(model, str string) string {
	wmodel := []rune(model)
	wstr := []rune(str)
	var isupper bool
	for i := range wstr {
		if i < len(wmodel) {
			isupper = unicode.IsUpper(wmodel[i])
		}
		if isupper {
			wstr[i] = unicode.ToUpper(wstr[i])
		}
	}
	return string(wstr)
}

func run() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		runProfiler(w, r, d)
	}
}

func runProfiler(w http.ResponseWriter, r *http.Request, d *service.Data) {
	// get language configruation
	config, err := gofiler.FindLanguage(languageDir, d.Project.Lang)
	if err != nil && err == gofiler.ErrorLanguageNotFound {
		service.ErrorResponse(w, http.StatusNotFound,
			"cannot profile: no such language: %s", d.Project.Lang)
		return
	}
	if err != nil {
		service.ErrorResponse(w, http.StatusInternalServerError,
			"cannot profile: %v", err)
		return
	}
	// start profiling job
	desc := jobs.Descriptor{BookID: d.Project.BookID, Name: "profiler"}
	jobID, err := jobs.Start(context.Background(), desc, func(ctx context.Context) error {
		var tokens []gofiler.Token
		err := eachLine(d.Project.BookID, func(line db.Chars) error {
			return eachWord(line, func(word db.Chars) error {
				tokens = append(tokens, gofiler.Token{OCR: word.OCR()})
				if word.IsFullyCorrected() {
					tokens[len(tokens)-1].COR = word.Cor()
				}
				return nil
			})
		})
		if err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		log.Debugf("profiling %d tokens", len(tokens))
		tmp, err := gofiler.Run(ctx, profbin, config.Path, tokens, logger{})
		if err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		profile := api.Profile{Profile: tmp, BookID: d.Project.BookID}

		if err := saveProfile(d.Project, profile); err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		if err := insertProfileIntoDB(profile); err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		return nil
	})
	if err != nil {
		service.ErrorResponse(w, http.StatusInternalServerError,
			"cannot profile: %v", err)
		return
	}
	service.JSONResponse(w, api.Job{ID: jobID})
}

func saveProfile(p *db.Project, profile api.Profile) (err error) {
	dest := filepath.Join(projectDir, p.Directory, "profile.json.gz")
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
	if err := json.NewEncoder(gzip).Encode(profile); err != nil {
		return fmt.Errorf("cannot write profile %s: %v", dest, err)
	}
	return nil
}

func insertProfileIntoDB(profile api.Profile) error {
	t := db.NewTransaction(service.Pool().Begin())
	t.Do(func(dtb db.DB) error {
		for _, table := range []string{"suggestions", "errorpatterns", "typcounts"} {
			stmnt := fmt.Sprintf("DELETE FROM %s WHERE bookid=?", table)
			if _, err := db.Exec(service.Pool(), stmnt, profile.BookID); err != nil {
				return fmt.Errorf("cannot insert profile: %v", err)
			}
		}
		types := make(map[string]int)
		for _, interp := range profile.Profile {
			if len(interp.Candidates) == 0 {
				continue
			}
			tid, err := insertInterpretation(dtb, interp, profile.BookID, types)
			if err != nil {
				return fmt.Errorf("cannot insert profile: %v", err)
			}
			for i, cand := range interp.Candidates {
				// skip if weight is too low
				if float64(cand.Weight) <= cutoff {
					continue
				}
				if err := insertCandidate(dtb, cand, profile.BookID,
					tid, types, i == 0); err != nil {
					return fmt.Errorf("cannot insert profile: %v", err)
				}
			}
		}
		stmnt := "UPDATE books SET statusid=? WHERE bookid=? AND statusid<?"
		status := db.StatusIDProfiled
		if _, err := db.Exec(service.Pool(), stmnt, status, profile.BookID, status); err != nil {
			return fmt.Errorf("cannot insert profile: %v", err)
		}
		return nil
	})
	return t.Done()
}

func insertInterpretation(
	dtb db.DB,
	interp gofiler.Interpretation,
	bid int,
	ids map[string]int,
) (int, error) {
	tid, err := db.NewType(dtb, interp.OCR, ids)
	if err != nil {
		return 0, fmt.Errorf("cannot insert interpretation: %v", err)
	}
	const stmnt = "INSERT INTO typcounts " +
		"(typid,bookid,counts) " +
		"VALUES (?,?,?) " +
		"ON DUPLICATE KEY UPDATE counts = counts + ?"
	_, err = db.Exec(dtb, stmnt, tid, bid, interp.N, interp.N)
	if err != nil {
		return 0, fmt.Errorf("cannot update counts: %v", err)
	}
	return tid, nil
}

func insertCandidate(
	dtb db.DB,
	cand gofiler.Candidate,
	bid, tid int,
	ids map[string]int,
	top bool,
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
	sid, err := db.NewType(dtb, cand.Suggestion, ids)
	if err != nil {
		return fmt.Errorf("cannot insert suggestion: %v", err)
	}
	mid, err := db.NewType(dtb, cand.Modern, ids)
	if err != nil {
		return fmt.Errorf("cannot insert modern: %v", err)
	}
	res, err := db.Exec(dtb, stmt, bid, tid, sid, mid,
		cand.Dict, cand.Weight, cand.Distance, top,
		patternString(cand.HistPatterns), patternString(cand.OCRPatterns))
	if err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	nid, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	if err := insertPatterns(dtb, cand.HistPatterns, bid, int(nid), false); err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
	}
	if err := insertPatterns(dtb, cand.OCRPatterns, bid, int(nid), true); err != nil {
		return fmt.Errorf("cannot insert candidate: %v", err)
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

func insertPatterns(dtb db.DB, ps []gofiler.Pattern, bid, sid int, ocr bool) error {
	const stmt = "INSERT INTO errorpatterns " +
		"(suggestionID,bookID,pattern,ocr) " +
		"VALUES (?,?,?,?)"
	for _, p := range ps {
		if _, err := db.Exec(dtb, stmt, sid, bid, p.Left+":"+p.Right, ocr); err != nil {
			return fmt.Errorf("cannot insert pattern: %v", err)
		}
	}
	return nil
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

func getPatterns() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		qs := r.URL.Query()["q"]
		h := strings.ToLower(r.URL.Query().Get("ocr"))
		var ocr bool
		if h == "true" || h == "1" {
			ocr = true
		}
		if len(qs) == 0 {
			getAllPatterns(w, d, ocr)
			return
		}
		queryPatterns(w, d, qs, ocr)
	}
}

func getAllPatterns(w http.ResponseWriter, d *service.Data, ocr bool) {
	const stmt = "SELECT p.pattern,COUNT(*) " +
		"FROM errorpatterns p " +
		"WHERE p.bookID=? AND p.ocr=? " +
		"GROUP BY p.pattern"
	rows, err := db.Query(service.Pool(), stmt, d.Project.BookID, ocr)
	if err != nil {
		service.ErrorResponse(w, http.StatusInternalServerError,
			"cannot get patterns: %v", err)
		return
	}
	defer rows.Close()
	patterns := api.PatternCounts{
		BookID: d.Project.BookID,
		OCR:    ocr,
		Counts: make(map[string]int),
	}
	for rows.Next() {
		var p string
		var c int
		if err := rows.Scan(&p, &c); err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot scan pattern: %v", err)
			return
		}
		patterns.Counts[p] = c
	}
	service.JSONResponse(w, patterns)
}

func queryPatterns(w http.ResponseWriter, d *service.Data, qs []string, ocr bool) {
	const stmt = "SELECT p.pattern,s.id,s.weight,s.distance," +
		"s.dict,s.histpatterns,s.ocrpatterns,s.topsuggestion,t1.typ,t2.typ,t3.typ " +
		"FROM errorpatterns p " +
		"JOIN suggestions s ON p.suggestionID=s.id " +
		"JOIN types t1 ON s.tokentypid=t1.id " +
		"JOIN types t2 on s.suggestiontypid=t2.id " +
		"JOIN types t3 on s.moderntypid=t3.id " +
		"WHERE p.bookID=? AND p.pattern=? AND p.ocr=?"
	res := api.Patterns{
		BookID:   d.Project.BookID,
		OCR:      ocr,
		Patterns: make(map[string][]api.Suggestion),
	}
	for _, q := range qs {
		rows, err := db.Query(service.Pool(), stmt, d.Project.BookID, q, ocr)
		if err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot query pattern %q: %v", q, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var p, h, o string
			var s api.Suggestion
			if err := rows.Scan(&p, &s.ID, &s.Weight, &s.Distance, &s.Dict,
				&h, &o, &s.Top, &s.Token, &s.Suggestion, &s.Modern); err != nil {
				service.ErrorResponse(w, http.StatusInternalServerError,
					"cannot query pattern %q: %v", q, err)
				return
			}
			s.HistPatterns = strings.Split(h, ",")
			s.OCRPatterns = strings.Split(o, ",")
			res.Patterns[p] = append(res.Patterns[p], s)
		}
	}
	service.JSONResponse(w, res)
}

func getSuspiciousWords() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		const stmt = "SELECT t.typ,tc.counts " +
			"FROM suggestions s " +
			"JOIN types t ON s.tokentypid=t.id " +
			"JOIN typcounts tc ON s.tokentypid=tc.typid AND s.bookid=tc.bookid " +
			"WHERE s.bookID=? AND s.topsuggestion=true AND s.distance > 0"
		rows, err := db.Query(service.Pool(), stmt, d.Project.BookID)
		if err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot get suspicious words: %v", err)
			return
		}
		defer rows.Close()
		patterns := api.SuggestionCounts{
			BookID:    d.Project.BookID,
			ProjectID: d.Project.ProjectID,
			Counts:    make(map[string]int),
		}
		for rows.Next() {
			var p string
			var c int
			if err := rows.Scan(&p, &c); err != nil {
				service.ErrorResponse(w, http.StatusInternalServerError,
					"cannot get suspicious words: %v", err)
				return
			}
			patterns.Counts[p] = c
		}
		service.JSONResponse(w, patterns)
	}
}

func getAdaptiveTokens() service.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		at := api.AdaptiveTokens{
			BookID:    d.Project.BookID,
			ProjectID: d.Project.ProjectID,
		}
		seen := make(map[string]bool)
		var i int
		eachLine(d.Project.BookID, func(line db.Chars) error {
			eachWord(line, func(word db.Chars) error {
				if i < 10 {
					i++
				}
				if word.IsFullyCorrected() {
					str := strings.ToLower(word.Cor())
					if !seen[str] {
						at.AdaptiveTokens = append(at.AdaptiveTokens, str)
						seen[str] = true
					}
				}
				return nil
			})
			return nil
		})
		service.JSONResponse(w, at)
	}
}
