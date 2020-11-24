package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

type server struct {
	host string
	path string
}

func (srvr *server) makeURL(id, format string) (filepath, filename string) {
	var ftpPath, ftpExtension, ftpPrefix, extension string

	switch format {
	case "pdb":
		ftpPath = "pdb/data/structures/divided/pdb/"
		ftpExtension = ".ent.gz"
		ftpPrefix = "pdb"
		extension = ".pdb"

	case "cif":
		ftpPath = "pdb/data/structures/divided/mmCIF/"
		ftpExtension = ".cif.gz"
		ftpPrefix = ""
		extension = ".cif"

		// case "emb", "map":
		// 	ftpPath = "emdb/structures/"
		// 	ftpExtension = ".map.gz"
		// 	ftpPrefix = "emd_"
		// 	extension = ".map"
	}

	filepath = path.Join(srvr.path, ftpPath, id[1:3], ftpPrefix+id+ftpExtension)
	filename = id + extension

	return
}

func (srvr *server) load(paths, filenames []string) error {
	c, err := ftp.Dial(srvr.host, ftp.DialWithTimeout(5*time.Second))

	if err != nil {
		return err
	}
	if err = c.Login("anonymous", "anonymous"); err != nil {
		return err
	}
	log.Println("Connected to ", srvr.host)
	for i, path := range paths {
		if err = retrieveFromConn(c, path, filenames[i]); err != nil {
			log.Printf("Error loading %s: %s", filenames[i], err)
		}
	}
	if err := c.Quit(); err != nil {
		return err
	}
	return nil
}

func (srvr *server) fetchPDB(idsRaw []string, format string) error {
	var paths, filenames []string

	ids, err := checkID(idsRaw)
	if err != nil {
		return err
	}
	for _, val := range ids {
		path, name := srvr.makeURL(val, format)
		paths = append(paths, path)
		filenames = append(filenames, name)
	}
	if err = srvr.load(paths, filenames); err != nil {
		return err
	}
	return nil
}

func retrieveFromConn(conn *ftp.ServerConn, path, filename string) error {
	bodyGz, err := conn.Retr(path)
	if err != nil {
		return err
	}
	// bar := pb.Full.Start64(1024 * 500)
	// barReader := bar.NewProxyReader(bodyGz)
	body, err := gzip.NewReader(bodyGz)
	if err != nil {
		return err
	}
	buf, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(filename, buf, 0644); err != nil {
		return err
	}
	log.Println("Loaded ", filename)
	bodyGz.Close()
	return nil
}

func checkID(ids []string) ([]string, error) {
	var res []string
	isNotDigit := func(c rune) bool { return c < '0' || c > '9' }

	for i, val := range ids {
		if len(val) != 4 || strings.IndexFunc(val, isNotDigit) == -1 {
			return nil, fmt.Errorf("error in pdb %d: %s", i+1, val)
		}
		res = append(res, strings.ToLower(val))

	}
	return res, nil
}

func main() {
	format := flag.String("format", "pdb", "format of file {pdb|cif}")
	region := flag.String("region", "us", "region of mirror {us|eu|jp}")
	flag.Usage = func() {
		fmt.Println("Usage: fetchpdb [-format] [-region] [id1...]")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()

	if len(args) < 2 {
		flag.Usage()
		return
	}

	var srvr server
	switch *region {
	case "jp":
		srvr = server{
			host: "ftp.pdbj.org:21",
			path: "/pub/",
		}
	case "eu":
		srvr = server{
			host: "ftp.ebi.ac.uk:21",
			path: "/pub/databases/rcsb/",
		}
	case "us":
		srvr = server{
			host: "ftp.wwpdb.org:21",
			path: "/pub/",
		}
	default:
		log.Fatal("no such mirror")
	}

	err := srvr.fetchPDB(args, *format)
	if err != nil {
		log.Fatal(err)
	}
}
