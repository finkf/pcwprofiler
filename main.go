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
	"regexp"
	"strings"
	"unicode"

	"github.com/finkf/gofiler"
	"github.com/finkf/pcwgo/api"
	"github.com/finkf/pcwgo/db"
	"github.com/finkf/pcwgo/jobs"
	"github.com/finkf/pcwgo/service"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

var (
	listen      = ":80"
	projectDir  = "/project-data"
	languageDir = "/language-data"
	profiler    = "/apps/profiler"
	dsn         = ""
	debug       = false
)

func init() {
	flag.StringVar(&listen, "listen", listen, "set host")
	flag.StringVar(&projectDir, "project-dir", projectDir, "set project base dir")
	flag.StringVar(&languageDir, "language-dir",
		languageDir, "set profiler's language backend")
	flag.StringVar(&profiler, "profiler",
		profiler, "path to profiler executable")
	flag.StringVar(&dsn, "dsn", dsn,
		"set mysql connection DSN (user:pass@proto(host)/dbname)")
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
			http.MethodGet, service.WithProject(getProfile()))))
	http.HandleFunc("/profile/jobs/books/",
		service.WithLog(service.WithMethods(
			http.MethodGet, service.WithProject(run()))))
	http.HandleFunc("/profile/jobs/",
		service.WithLog(service.WithMethods(http.MethodGet, status())))
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

func status() service.HandlerFunc {
	re := regexp.MustCompile(`/jobs/(\d+)$`)
	return func(w http.ResponseWriter, r *http.Request, d *service.Data) {
		var jobID int
		if err := service.ParseIDs(r.URL.String(), re, &jobID); err != nil {
			service.ErrorResponse(w, http.StatusNotFound, "invalid job id: %v", err)
			return
		}
		service.JSONResponse(w, jobs.Job(jobID))
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
	http.ServeFile(w, r, filepath.Join(p.Directory, "profile.json.gz"))
}

func queryProfile(w http.ResponseWriter, p *db.Project, qs []string) {
	// return the suggestion for the given query terms
	const stmt = "SELECT id FROM types WHERE typ=?"
	suggestions := api.Suggestions{BookID: p.BookID}
	for _, q := range qs {
		ql := strings.ToLower(q)
		rows, err := db.Query(service.Pool(), stmt, ql)
		if err != nil {
			service.ErrorResponse(w, http.StatusInternalServerError,
				"cannot get suggestions: %v", err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var qid int
			if err := rows.Scan(&qid); err != nil {
				service.ErrorResponse(w, http.StatusInternalServerError,
					"cannot get suggestions: %v", err)
				return
			}
			if err := selectSuggestions(q, p.BookID, qid, &suggestions); err != nil {
				service.ErrorResponse(w, http.StatusInternalServerError,
					"cannot get suggestions: %v", err)
				return
			}
		}
	}
	service.JSONResponse(w, suggestions)
}

func selectSuggestions(q string, bid, qid int, suggs *api.Suggestions) error {
	const stmt = "SELECT s.Weight,s.Distance,s.TopSuggestion,t.Typ FROM suggestions s " +
		"JOIN types t ON s.suggestiontypid=t.id WHERE s.BookID=? AND s.TypID=?"
	rows, err := db.Query(service.Pool(), stmt, bid, qid)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		s := api.Suggestion{Token: q}
		if err := rows.Scan(&s.Weight, &s.Distance, &s.Top, &s.Suggestion); err != nil {
			return err
		}
		s.Suggestion = sameCasing(q, s.Suggestion)
		suggs.Suggestions = append(suggs.Suggestions, s)
	}
	return nil
}

func sameCasing(model, str string) string {
	wmodel := []rune(model)
	wstr := []rune(str)
	for i := range wmodel {
		if i >= len(wstr) {
			break
		}
		if unicode.IsUpper(wmodel[i]) {
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
	jobID, err := jobs.Start(context.Background(), d.Project.BookID, func(ctx context.Context) error {
		var tokens []gofiler.Token
		err := eachLine(d.Project.BookID, func(line db.Chars) error {
			for _, token := range tokenize(line) {
				tokens = append(tokens, gofiler.Token{
					COR: token.Cor(),
					OCR: token.OCR(),
				})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		log.Debugf("profiling %d tokens", len(tokens))
		profile, err := gofiler.Run(ctx, profiler, config.Path, tokens, logger{})
		if err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		if err := saveProfile(d.Project, profile); err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		if err := insertProfileIntoDB(profile, d.Project.BookID); err != nil {
			return fmt.Errorf("cannot profile: %v", err)
		}
		return nil
	})
	if err != nil {
		service.ErrorResponse(w, http.StatusInternalServerError,
			"cannot profile: %v", err)
		return
	}
	service.JSONResponse(w, struct {
		JobID int `json:"jobId"`
	}{jobID})
}

func saveProfile(p *db.Project, profile gofiler.Profile) (err error) {
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

func insertProfileIntoDB(profile gofiler.Profile, bookID int) error {
	t := db.NewTransaction(service.Pool().Begin())
	const stmt = "INSERT INTO suggestions " +
		"(BookID,PageID,LineID,TokenID,TypID,SuggestionTypID,Weight,Distance,TopSuggestion) " +
		"VALUES (?,?,?,?,?,?,?,?,?)"
	t.Do(func(dtb db.DB) error {
		for _, interp := range profile {
			if len(interp.Candidates) == 0 {
				continue
			}
			oid, err := db.NewType(dtb, interp.OCR, nil)
			if err != nil {
				return fmt.Errorf("cannot insert profile: %v", err)
			}
			for i, cand := range interp.Candidates {
				if cand.Distance == 0 { // skip if no OCR error
					continue
				}
				sid, err := db.NewType(dtb, cand.Suggestion, nil)
				if err != nil {
					return fmt.Errorf("cannot insert profile: %v", err)
				}
				res, err := dtb.Exec(stmt, bookID, 1, 1, 1, oid, sid, cand.Weight, cand.Distance, i == 0)
				if err != nil {
					return fmt.Errorf("cannot insert profile: %v", err)
				}
				suggID, err := res.LastInsertId()
				if err := insertPatternsIntoDB(
					dtb, cand.OCRPatterns, bookID, int(suggID), false); err != nil {
					return fmt.Errorf("cannot insert profile: %v", err)
				}
			}
		}
		return nil
	})
	return t.Done()
}

func insertPatternsIntoDB(dtb db.DB, ps []gofiler.Pattern, bookID, suggID int, ocr bool) error {
	const stmt = "INSERT INTO errorpatterns (suggestionID,bookID,pattern,ocr) VALUES (?,?,?,?)"

	for _, p := range ps {
		if _, err := db.Exec(dtb, stmt, suggID, bookID, p.Left+":"+p.Right, ocr); err != nil {
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

func tokenize(line db.Chars) []db.Chars {
	var tokens []db.Chars
	// log.Debugf("tokenizing line: [%s] %s", line.OCR(), line.Cor())
	for t, r := line.NextWord(); len(t) > 0 && len(r) > 0; t, r = r.NextWord() {
		// log.Debugf("word: [%s] %s", t.OCR(), t.Cor())
		tokens = append(tokens, t)
	}
	return tokens
}

type logger struct{}

func (logger) Log(msg string) {
	log.Debug(msg)
}
