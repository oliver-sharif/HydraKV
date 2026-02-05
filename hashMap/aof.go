package hashMap

import (
	"bufio"
	"encoding/binary"
	"hydrakv/envhandler"
	"io"
	"log"
	"os"
	"strings"
	"time"
	"unsafe"
)

type Data struct {
	Action string
	Key    string
	Value  string
	Ttl    int64
}

type AOFEntry struct {
	Key   string
	Value string
	Ttl   int64
}

type AOF struct {
	com         chan Data
	quit        chan bool
	compressing chan struct{}
	FileName    string
	file        *bufio.Writer
	iofile      *os.File
	readBuf     []byte
	aeCB        func() []*AOFEntry
}

// NewAOF creates a new AOF
func NewAOF(file string, cbFunc func() []*AOFEntry) (*AOF, error) {
	// first check if the Aof dir exists - if not create it
	if _, err := os.Stat(*envhandler.ENV.DB_FOLDER); err != nil {
		// dir does not exist - create it
		err := os.Mkdir(*envhandler.ENV.DB_FOLDER, 0755)
		if err != nil {
			return nil, err
		}
	}

	// the file is .Aof/file.bin
	file = *envhandler.ENV.DB_FOLDER + "/" + file + ".bin"

	// creat ethe AOF structure
	aof := &AOF{
		com: make(chan Data, 100000), quit: make(chan bool), FileName: file, compressing: make(chan struct{}), aeCB: cbFunc,
	}

	// Create the structure
	return aof, nil
}

// Start starts the AOF loop
func (a *AOF) Start() error {
	// open the file in binary mode
	f, err := os.OpenFile(a.FileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	a.iofile = f
	a.file = bufio.NewWriterSize(f, 1024*64)

	// start the loop
	go a.Loop()
	return nil
}

// writeFrame, writes a GOB frame to the file
func (a *AOF) writeFrame(data Data) error {
	// Write Action
	if err := binary.Write(a.file, binary.BigEndian, uint32(len(data.Action))); err != nil {
		return err
	}
	if len(data.Action) > 0 {
		ptr := unsafe.StringData(data.Action)
		if _, err := a.file.Write(unsafe.Slice(ptr, len(data.Action))); err != nil {
			return err
		}
	}

	// Write Key
	if err := binary.Write(a.file, binary.BigEndian, uint32(len(data.Key))); err != nil {
		return err
	}
	if len(data.Key) > 0 {
		ptr := unsafe.StringData(data.Key)
		if _, err := a.file.Write(unsafe.Slice(ptr, len(data.Key))); err != nil {
			return err
		}
	}

	// Write Value
	if err := binary.Write(a.file, binary.BigEndian, uint32(len(data.Value))); err != nil {
		return err
	}
	if len(data.Value) > 0 {
		ptr := unsafe.StringData(data.Value)
		if _, err := a.file.Write(unsafe.Slice(ptr, len(data.Value))); err != nil {
			return err
		}
	}

	// Write TTL
	if err := binary.Write(a.file, binary.BigEndian, data.Ttl); err != nil {
		return err
	}

	return nil
}

func (a *AOF) readFrame(r io.Reader, data *Data) error {
	if a.readBuf == nil {
		a.readBuf = make([]byte, 4096)
	}

	var sizeBuf [4]byte

	// Read Action
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if int(size) > len(a.readBuf) {
		a.readBuf = make([]byte, size)
	}
	if size > 0 {
		if _, err := io.ReadFull(r, a.readBuf[:size]); err != nil {
			return err
		}
		data.Action = string(a.readBuf[:size])
	} else {
		data.Action = ""
	}

	// Read Key
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return err
	}
	size = binary.BigEndian.Uint32(sizeBuf[:])
	if int(size) > len(a.readBuf) {
		a.readBuf = make([]byte, size)
	}
	if size > 0 {
		if _, err := io.ReadFull(r, a.readBuf[:size]); err != nil {
			return err
		}
		data.Key = string(a.readBuf[:size])
	} else {
		data.Key = ""
	}

	// Read Value
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return err
	}
	size = binary.BigEndian.Uint32(sizeBuf[:])
	if int(size) > len(a.readBuf) {
		a.readBuf = make([]byte, size)
	}
	if size > 0 {
		if _, err := io.ReadFull(r, a.readBuf[:size]); err != nil {
			return err
		}
		data.Value = string(a.readBuf[:size])
	} else {
		data.Value = ""
	}

	// Read TTL
	if err := binary.Read(r, binary.BigEndian, &data.Ttl); err != nil {
		return err
	}

	return nil
}

// Close closes the AOF and waits for the loop to finish
func (a *AOF) Close() error {
	close(a.com)
	<-a.quit
	log.Printf("AOF file %s closed", a.FileName)
	return a.iofile.Close()
}

// Loop reads the data comming from the channel and writes it to the file
func (a *AOF) Loop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	// This is necessary to wait for all items to be written when system goes down
	defer ticker.Stop()

	for {
		select {
		case d, ok := <-a.com:
			if !ok {
				a.file.Flush()
				a.iofile.Sync()
				close(a.quit)
				return
			}
			err := a.writeFrame(d)
			if err != nil {
				log.Println("Error writing to AOF:", err)
			}
		case <-ticker.C:
			// flush only when the buffer is filled
			if a.file.Buffered() > 0 {
				a.file.Flush()
				a.iofile.Sync()
			}
		case <-a.compressing:
			// Data to create a new AOF bin File - this is a callback to HashMap to get the entries
			// it blocks writes to the Aof file until the compression is done
			a.createCompressedAOF(a.aeCB())
		}
	}
}

// createCompressedAOF creates a new AOF file with compressed entries and replaces
// the old file in an atomic, crash-safe way.
func (a *AOF) createCompressedAOF(entries []*AOFEntry) {

	tmpName := strings.Split(a.FileName, ".")[0] + ".tmp.bin"

	// 1. Create temp file
	tmpFile, err := os.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("cannot create compressed AOF file! " + err.Error())
		return
	}
	tmpBuf := bufio.NewWriterSize(tmpFile, 1024*1024*16)

	// 2. Write all entries to tmp file
	for _, e := range entries {

		// write action "set"
		if err := binary.Write(tmpBuf, binary.BigEndian, uint32(len("set"))); err != nil {
			log.Println("error writing action to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}
		ptr := unsafe.StringData("set")
		if _, err := tmpBuf.Write(unsafe.Slice(ptr, len("set"))); err != nil {
			log.Println("error writing action string to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}

		// write key
		if err := binary.Write(tmpBuf, binary.BigEndian, uint32(len(e.Key))); err != nil {
			log.Println("error writing key length to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}
		ptr = unsafe.StringData(e.Key)
		if _, err := tmpBuf.Write(unsafe.Slice(ptr, len(e.Key))); err != nil {
			log.Println("error writing key to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}

		// write value
		if err := binary.Write(tmpBuf, binary.BigEndian, uint32(len(e.Value))); err != nil {
			log.Println("error writing value length to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}
		ptr = unsafe.StringData(e.Value)
		if _, err := tmpBuf.Write(unsafe.Slice(ptr, len(e.Value))); err != nil {
			log.Println("error writing value to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}

		// write ttl
		if err := binary.Write(tmpBuf, binary.BigEndian, e.Ttl); err != nil {
			log.Println("error writing ttl to tmp AOF! " + err.Error())
			tmpFile.Close()
			return
		}
	}

	// 3. Flush + fsync tmp file
	if err := tmpBuf.Flush(); err != nil {
		log.Println("error flushing tmp AOF buffer! " + err.Error())
		tmpFile.Close()
		return
	}
	if err := tmpFile.Sync(); err != nil {
		log.Println("error syncing tmp AOF file! " + err.Error())
		tmpFile.Close()
		return
	}
	tmpFile.Close() // safe to close

	// 4. Finish writing to the old file: flush + fsync + close
	a.file.Flush()
	a.iofile.Sync()
	a.iofile.Close()

	// 5. Atomically replace old file with tmp file
	// rename() is atomic on POSIX systems.
	if err := os.Rename(tmpName, a.FileName); err != nil {
		log.Println("cannot atomically rename tmp AOF! " + err.Error())
		return
	}

	// 6. Re-open the new AOF file
	a.iofile, err = os.OpenFile(a.FileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Println("cannot reopen new AOF file! " + err.Error())
		return
	}
	a.file = bufio.NewWriterSize(a.iofile, 1024*64)

	log.Println("Compressed AOF file created")
}
