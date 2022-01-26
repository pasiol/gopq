package gopq

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
	Updated         = false
	Debug           = false
	PrimusQueryPath = "./primusquery"
)

func stringWithCharset(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func createFile(filename string, content string) error {
	err := ioutil.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		if Debug {
			log.Printf("creating the file failed: %s", err)
		}
		return err
	}
	return nil
}

func createTMPFile(filename string, content string) (string, error) {
	tmpfile, err := ioutil.TempFile("", filename)
	if err != nil {
		if Debug {
			log.Printf("creating tmp-file failed")
		}
		return "", err
	}
	_, err = tmpfile.WriteString(content)
	if err != nil {
		_ = tmpfile.Close()
		if Debug {
			log.Printf("writing on the tmp-file failed: %s", err)
		}
		return "", err
	}
	err = tmpfile.Close()
	if err != nil {
		if Debug {
			log.Printf("closing the tmp-file failed")
		}
		return "", err
	}
	return tmpfile.Name(), nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func safeDelete(filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR, 0666)

	if err != nil {
		if Debug {
			log.Printf("system failure, cannot open file %s: %s", filename, err)
		}
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("system failure, cannot read file: %s", filename)
		return err
	}

	var size int64 = fileInfo.Size()
	zeroBytes := make([]byte, size)

	for i := 0; i < 10; i++ {
		copy(zeroBytes[:], strconv.Itoa(i))
		_, err := file.Write([]byte(zeroBytes))
		if err != nil {
			log.Fatalf("system failure, cannot write on file: %s", filename)
			return err
		}
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("system failure, cannot close file: %s", filename)
		return err
	}

	err = os.Remove(filename)
	if err != nil {
		log.Fatalf("system failure, cannot remove file: %s", filename)
		return err
	}

	return nil
}

func RemoveFile(filename string) error {
	err := os.Remove(filename)

	if err != nil {
		if Debug {
			log.Printf("removing file %s failed: %s", filename, err)
		}
		return err
	}
	return nil
}

func SetQuery(query PrimusQuery) string {
	queryString := "#CHARSET " + query.Charset + "\n"
	queryString = queryString + "#HOST " + query.Host + "\n"
	queryString = queryString + "#PORT " + query.Port + "\n"
	queryString = queryString + "#USER " + query.User + "\n"
	queryString = queryString + "#PASS " + query.Pass + "\n"
	queryString = queryString + "#OUTPUT " + query.Output + "\n"
	queryString = queryString + "#DATABASE " + query.Database + "\n"
	queryString = queryString + "#SEARCH " + query.Search + "\n"
	queryString = queryString + "#SORT " + "V1" + "\n"
	if query.Header != "" {
		queryString = queryString + "#HEADER_START\n" + query.Header + "\n#HEADER_STOP\n"
	}
	queryString = queryString + query.Data + "\n"
	if query.Footer != "" {
		queryString = queryString + "#FOOTER_START\n" + query.Footer + "\n#FOOTER_STOP\n"
	}
	return queryString
}

func RepairPrimusGeneratedJSON(f string) error {
	// TODO: more complicated JSON-arrays
	jsonAsBytes, err := ioutil.ReadFile(f)
	if err != nil {
		log.Fatalf("cannot read %s JSON-file: %s", f, err)
		return err
	}

	jsonAsString := string(jsonAsBytes)
	if strings.Contains(jsonAsString, ",") {
		err := safeDelete(f)
		if err != nil {
			return err
		}

		time.Sleep(2 * time.Second)
		end := len(jsonAsString) - 6
		repairedJSON := jsonAsString[0:end] + "\n]"
		err = createFile(f, repairedJSON)
		if err != nil {
			return err
		}
	}

	return nil
}

func CountPQErrors(output string) (int, error) {
	errorsPattern := regexp.MustCompile(`Errors: ([0-9])+`)
	matches := errorsPattern.Find([]byte(output))
	if len(matches) >= 1 {
		errorCountPattern := regexp.MustCompile(`([0-9])+`)
		numbers := string(errorCountPattern.Find(matches))
		count, err := strconv.Atoi(numbers)
		if err != nil {
			return -1, err
		}
		return count, err
	}
	return 0, nil
}

func NewCardID(output string) (int, error) {
	newCardPattern := regexp.MustCompile(`NEW: ([0-9])+`)
	founded := newCardPattern.Find([]byte(output))
	if len(founded) >= 1 {
		cardIDPattern := regexp.MustCompile(`([0-9])+`)
		number := string(cardIDPattern.Find(founded))
		cardID, err := strconv.Atoi(number)
		if err != nil {
			return -1, err
		}
		return cardID, nil
	}
	return -1, nil
}

func UpdatePQ(host string) error {
	if !Updated {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, PrimusQueryPath, host, "-update")
		out, err := cmd.Output()
		if ctx.Err() == context.DeadlineExceeded {
			return errors.New("primusquery update timeout")
		}
		if err != nil {
			if Debug {
				log.Printf("PQ update fails: %s", err)
			}
			return err
		}
		if Debug {
			log.Printf("update output: %s", out)
		}
	} else {
		if Debug {
			log.Print("PQ already updated")
		}
	}
	return nil
}

func ExecuteImportQuery(filename string, primusHost, primusPort, userName string, password string, loaderName string) (string, error) {
	if fileExists(filename) {
		output, err := exec.Command(PrimusQueryPath, primusHost, primusPort, userName, password, loaderName, "-i", filename).Output()
		if err != nil {
			if Debug {
				log.Printf("import query %s failed: %s", loaderName, err)
			} else {
				_ = safeDelete(filename)
			}
			return "", err
		} else if len(output) > 0 && Debug {
			log.Printf("import query %s output: %s", loaderName, output)
		}
		_ = safeDelete(filename)
		return string(output), err
	} else {
		if Debug {
			log.Printf("%s import-file %s not exists", loaderName, filename)
		}
		return "", errors.New("import-file not exists")
	}
}

func ExecuteAtomicImportQuery(filename string, primusHost, primusPort, userName string, password string, loaderName string) (int, int, error) {
	// todo: check import-file content and validity, one card element
	output, err := ExecuteImportQuery(filename, primusHost, primusPort, userName, password, loaderName)
	var (
		newCardID  int
		errorCount int
	)
	if err == nil {
		newCardID, err = NewCardID(output)
		if err != nil {
			if Debug {
				log.Printf("executing atomic import query %s failed: %s", loaderName, err)
			}
			return -1, -1, err
		}
		errorCount, err = CountPQErrors(output)
		if err != nil {
			if Debug {
				log.Printf("executing atomic import query %s failed: %s", loaderName, err)
			}
			return -1, -1, err
		}
	}
	return newCardID, errorCount, nil
}

func ExecuteAndRead(query PrimusQuery, timeout int) (string, error) {
	if !Updated {
		err := UpdatePQ(query.Host)
		if err != nil {
			return "", err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	query.Output = ""
	queryText := SetQuery(query)

	queryFilename := stringWithCharset(128)
	queryFilename, err := createTMPFile(queryFilename, queryText)
	if err != nil {
		return "", err
	}
	if Debug {
		_ = createFile("debug.priq", queryText)
	}

	cmd := exec.CommandContext(ctx, PrimusQueryPath, queryFilename)
	out, err := cmd.Output()
	if ctx.Err() == context.DeadlineExceeded {
		if Debug {
			log.Printf("primus connection timeout: %s", err)
		}
		safeDelete(queryFilename)
		return "", err
	}

	err = safeDelete(queryFilename)
	if err != nil {
		return string(out), err
	}

	if Debug {
		log.Printf("execute output: %s", string(out[:]))
	}
	return string(out), nil
}

func Execute(query PrimusQuery, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	queryText := SetQuery(query)
	queryFilename := stringWithCharset(128)
	queryFilename, err := createTMPFile(queryFilename, queryText)
	if err != nil {
		return err
	}
	if Debug {
		_ = createFile("debug.priq", queryText)
	}

	cmd := exec.CommandContext(ctx, PrimusQueryPath, queryFilename)
	out, err := cmd.Output()
	if ctx.Err() == context.DeadlineExceeded {
		if Debug {
			log.Printf("primus connection timeout: %s", err)
		}
		safeDelete(queryFilename)
		return err
	}

	err = safeDelete(queryFilename)
	if err != nil {
		return err
	}
	if Debug {
		log.Printf("execute output: %s", string(out[:]))
	}
	return nil
}
