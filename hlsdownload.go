package hlsdownload

import (
	"bufio"
	"fmt"
	"github.com/isaacml/cmdline"
	"github.com/todostreaming/cola"
	"github.com/todostreaming/m3u8pls"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	fiforoot     = "/var/segments/" // /tmp/
	queuetimeout = 60               // creamos una cola con un timeout de 1 minuto = 60 secs
)

var (
	Warning *log.Logger
	fw      *os.File //FIFO File descriptor
)

func init() {
	Warning = log.New(os.Stderr, "\n\n[WARNING]: ", log.Ldate|log.Ltime|log.Lshortfile)
}

type Status struct {
	Running bool // proceso completo funcionando
	Segnum  int  // numero de segmento bajado
	Kbps    int  // download kbps speed
	Fails   int  // m3u8 sucesive fails
	Badfifo bool
	Paused  bool
	Lastime int64 // last UNIX time an m3u8 was downloaded
}

type HLSDownload struct {
	duration      []float64  // matrices de duracion en segundos de los segmentos play?.ts
	downloaddir   string     // directorio RAMdisk donde se guardan los ficheros bajados del server y listos para reproducir
	m3u8          string     // playlist HLS *.m3u8 a bajarse para playback
	downloading   bool       // esta bajando segmentos
	running       bool       // proceso completo funcionando
	paused        bool       // director pausado, FIFO desbloqueado, downloader funcionando aún
	execpause     bool       // pausa ejecutada por el director, director esperando
	mu_seg        sync.Mutex // Mutex para las variables internas del objeto HLSPlay
	segnum        int        // numero del segmento actual en el orden de bajada
	numsegs       int
	lastTargetdur float64
	lastMediaseq  int64
	badfifo       bool             // bad fifo indicator
	lastIndex     int              // index del segmento donde toca copiar download.ts  entre 0 y numsegs-1
	lastPlay      int              // index del segmento que se envió al secuenciador desde el director
	lastkbps      int              // download kbps speed
	m3u8pls       *m3u8pls.M3U8pls // parser M3U8
	m3u8fail      int              //numero de veces sucesivas que no se baja un m3u8 correcto
	lastm3u8      int64            // last UNIX time that an m3u8 was downloaded
	cola          *cola.Cola       // cola con los segments/dur para bajar
	mu_play       []sync.Mutex     // Mutex para la escritura/lectura de segmentos *.ts cíclicos
}

func HLSDownloader(m3u8, downloaddir string) *HLSDownload {
	hls := &HLSDownload{}
	hls.mu_seg.Lock()
	defer hls.mu_seg.Unlock()
	hls.downloaddir = downloaddir
	hls.m3u8 = m3u8
	hls.downloading = false
	hls.running = false
	hls.lastTargetdur = 0.0
	hls.lastMediaseq = 0
	hls.lastIndex = 0
	hls.segnum = 0
	hls.badfifo = false
	hls.paused = false
	hls.execpause = false
	hls.lastPlay = 0
	hls.lastkbps = 0
	hls.m3u8fail = 0
	hls.m3u8pls = m3u8pls.M3U8playlist(hls.m3u8)
	hls.cola = cola.CreateQueue(queuetimeout)
	// calculamos los segmentos máximos que caben
	hls.numsegs = 7                               // 7 segmentos cíclicos
	hls.mu_play = make([]sync.Mutex, hls.numsegs) // segmentos de maximo 12 segundos a 5 Mbps
	hls.duration = make([]float64, hls.numsegs)

	return hls
}

func (h *HLSDownload) m3u8parser() {
	for {
		h.cola.Keeping()
		h.m3u8pls.Parse()  // bajamos y parseamos la url m3u8 HLS a reproducir
		if !h.m3u8pls.Ok { // m3u8 no accesible o explotable
			h.mu_seg.Lock()
			h.m3u8fail++
			h.mu_seg.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}
		// aqui el m3u8 ha bajado correctamente
		h.mu_seg.Lock()
		h.lastm3u8 = time.Now().Unix()
		h.m3u8fail = 0
		if !h.running {
			h.mu_seg.Unlock()
			break
		}
		if h.m3u8pls.Mediaseq == h.lastMediaseq { // no ha cambiado el m3u8 aún
			h.mu_seg.Unlock()
			time.Sleep(time.Duration(h.m3u8pls.Targetdur/2.0) * time.Second)
			continue
		}
		h.lastMediaseq = h.m3u8pls.Mediaseq
		h.lastTargetdur = h.m3u8pls.Targetdur
		for k, v := range h.m3u8pls.Segment { // segmento
			h.cola.Add(v, h.m3u8pls.Duration[k])
		}
		h.mu_seg.Unlock()
		////h.cola.Print()

		time.Sleep(time.Duration(h.m3u8pls.Targetdur) * time.Second)
	}
}

func (h *HLSDownload) downloader() {
	started := true
	for {
		h.mu_seg.Lock()
		if !h.running {
			h.mu_seg.Unlock()
			break
		}
		h.mu_seg.Unlock()

		segname, segdur, next := h.cola.Next()
		if !next {
			time.Sleep(1 * time.Second)
			continue
		}

		os.Remove(h.downloaddir + "download.ts")
		kbps, ok := download(h.downloaddir+"download.ts", segname, segdur)
		if !ok {
			runtime.Gosched()
			continue
		}
		// aqui el segmento ts ha bajado correctamente

		h.mu_seg.Lock()
		h.segnum++
		h.lastkbps = kbps
		h.mu_seg.Unlock()
		if started {
			started = false
			// copiar numsegs veces el segmento download.ts
			for i := 0; i < h.numsegs; i++ {
				h.mu_seg.Lock()
				h.duration[i] = segdur
				h.mu_seg.Unlock()
				cp := fmt.Sprintf("cp -f %sdownload.ts %splay%d.ts", h.downloaddir, h.downloaddir, i)
				////fmt.Printf("[downloader] - 4 => %s\n",cp)
				h.mu_play[i].Lock()
				exec.Command("/bin/sh", "-c", cp).Run()
				h.mu_play[i].Unlock()
			}
		} else {
			// copiar solo una vez donde corresponde download.ts
			h.mu_seg.Lock()
			h.lastIndex++
			if h.lastIndex >= h.numsegs {
				h.lastIndex = 0
			}
			i := h.lastIndex
			h.duration[i] = segdur
			h.mu_seg.Unlock()

			cp := fmt.Sprintf("cp -f %sdownload.ts %splay%d.ts", h.downloaddir, h.downloaddir, i)
			//fmt.Printf("[downloader lock %d] => %s\n", i, cp) ////=>
			h.mu_play[i].Lock()
			exec.Command("/bin/sh", "-c", cp).Run()
			h.mu_play[i].Unlock()
			//fmt.Printf("[downloader unlock %d] => %s\n", i, cp) ////=>
		}
		runtime.Gosched()
	}
}

// baja un segmento al fichero download y lo reintenta 3 veces con un timeout 2 * segdur
// download es la direccion absoluta del fichero donde bajarlo
// segname es la URL completa del fichero a bajar
// segdur es la duración media del fichero (importante para el timeout)
// devuelve kbps de download y ok
func download(download, segname string, segdur float64) (int, bool) {
	var bytes int64
	var downloaded, downloadedok bool
	var kbps int
	var downloading bool

	cmd := fmt.Sprintf("/usr/bin/wget -t 3 --limit-rate=625k -S -O %s %s", download, segname)
	////fmt.Println(cmd)
	exe := cmdline.Cmdline(cmd)

	lectura, err := exe.StderrPipe()
	if err != nil {
		Warning.Println(err)
	}
	mReader := bufio.NewReader(lectura)
	tiempo := time.Now().Unix()
	go func() {
		for {
			if (time.Now().Unix()-tiempo) > int64(segdur) && downloading {
				exe.Stop()
				////fmt.Println("[download] WGET matado supera los XXX segundos !!!!")
				break
			}
			time.Sleep(1 * time.Second)
		}
	}()
	downloading = true
	ns := time.Now().UnixNano()
	exe.Start()
	for { // bucle de reproduccion normal
		line, err := mReader.ReadString('\n')
		if err != nil {
			////fmt.Println("Fin del wget !!!")
			break
		}
		line = strings.TrimRight(line, "\n")
		if strings.Contains(line, "HTTP/1.1 200 OK") {
			////fmt.Println("[downloader] Downloaded OK")
			downloaded = true
		}
		if strings.Contains(line, "Content-Length:") { //   Content-Length: 549252
			line = strings.Trim(line, " ")
			fmt.Sscanf(line, "Content-Length: %d", &bytes)
		}
		////fmt.Printf("[wget] %s\n", line) //==>
	}
	exe.Stop()
	downloading = false
	ns = time.Now().UnixNano() - ns

	if downloaded {
		// comprobar que el fichero se ha bajado correctamente
		fileinfo, err := os.Stat(download) // fileinfo.Size()
		if err != nil {
			downloadedok = false
			Warning.Println(err)
		}
		filesize := fileinfo.Size()
		if filesize == int64(bytes) {
			downloadedok = true
		} else {
			downloadedok = false
		}
		if ns != 0 { // evitar un 0 divisor
			kbps = int(filesize * 8.0 * 1e9 / ns / 1000.0)
		}
	}

	return kbps, downloadedok
}

// esta funcion envia los ficheros a reproducir a la cola de reproducción en el FIFO1 /tmp/fifo1
// secuencia /tmp/fifo1
func (h *HLSDownload) secuenciador(file string, indexPlay int) error {

	h.mu_play[indexPlay].Lock()
	defer h.mu_play[indexPlay].Unlock()

	fr, err := os.Open(file) // read-only
	if err != nil {
		Warning.Println(err)
		return err
	}
	defer fr.Close()
	if _, err := io.Copy(fw, fr); err == nil { // possible issue when fw is closed
		////fmt.Printf("[secuenciador] (%s) Copiados %d bytes\n", file, n) // copia perfecta sin fallos
	} else {
		Warning.Println(err) // no salimos en caso de error de copia en algun momento
		h.mu_seg.Lock()
		h.badfifo = true
		h.mu_seg.Unlock()
	}

	return err
}

func (h *HLSDownload) director() {
	started := true
	for {
		if started {
			started = false
			segnum := 0
			for segnum < 3 {
				h.mu_seg.Lock()
				segnum = h.segnum
				h.mu_seg.Unlock()
				time.Sleep(1 * time.Second)
			}
		}

		h.mu_seg.Lock()
		if !h.running {
			h.mu_seg.Unlock()
			break
		}

		if h.paused {
			h.execpause = true
			h.mu_seg.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		h.execpause = false
		indexplay := h.lastPlay
		h.mu_seg.Unlock()

		file := fmt.Sprintf("%splay%d.ts", h.downloaddir, indexplay)
		//fmt.Printf("[director lock %d] => Play %s\n", indexplay, file) ////=>
		err := h.secuenciador(file, indexplay)
		if err != nil { // si pasa por aqui se supone que el FIFO1 esta muerto, y reintenta hasta que reviva cada segundo
			Warning.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		//fmt.Printf("[director unlock %d] => Play %s\n", indexplay, file) ////=>

		h.mu_seg.Lock()
		h.lastPlay++
		if h.lastPlay >= h.numsegs {
			h.lastPlay = 0
		}
		h.mu_seg.Unlock()

		runtime.Gosched()

	}
}

func (h *HLSDownload) Stop() error {
	var err error

	h.mu_seg.Lock()
	defer h.mu_seg.Unlock()
	if !h.running {
		return fmt.Errorf("hlsplay: ALREADY_STOPPED_ERROR")
	}
	h.downloading = false
	h.running = false
	h.lastTargetdur = 0.0
	h.lastMediaseq = 0
	h.lastIndex = 0
	h.lastPlay = 0
	h.m3u8fail = 0
	h.lastkbps = 0
	h.segnum = 0
	h.lastm3u8 = 0
	h.badfifo = false
	h.paused = false
	h.execpause = false
	h.cola = cola.CreateQueue(queuetimeout)
	h.duration = make([]float64, h.numsegs)
	fw.Close()

	return err
}

// you dont need to call this func less than secondly
func (h *HLSDownload) Status() *Status {
	var st Status

	h.mu_seg.Lock()
	defer h.mu_seg.Unlock()

	st.Running = h.running // downloader + director corriendo
	st.Segnum = h.segnum
	st.Kbps = h.lastkbps
	st.Fails = h.m3u8fail
	st.Badfifo = h.badfifo
	st.Paused = h.execpause // realmente FIFO parado (downloader corriendo)
	st.Lastime = h.lastm3u8

	return &st
}

// Pauses the director once it flushes all the FIFO out
// Paused is activated once the flushes ends
func (h *HLSDownload) Pause() {
	h.mu_seg.Lock()
	defer h.mu_seg.Unlock()

	if h.running {
		h.paused = true
	}
}

// Unpauses a completed flushed out director
// if u pause a hlsdownload, you will not be able to resume it until it flushes completely the FIFO
// so u will need to read the Status before using this method efficiently
// resumes the FIFO writing with the next segment in the queue
func (h *HLSDownload) Resume() {
	h.mu_seg.Lock()
	defer h.mu_seg.Unlock()

	if h.running && h.execpause && h.paused {
		h.paused = false
	}
}

// call this func after Pause(), will block until FIFO is empty
func (h *HLSDownload) WaitforPaused() error {
	var err error

	for {
		h.mu_seg.Lock()
		paused := h.execpause
		h.mu_seg.Unlock()
		if paused {
			break
		}
		time.Sleep(50 * time.Millisecond)
		runtime.Gosched()
	}

	return err
}

func (h *HLSDownload) Run() error {
	var err error

	h.mu_seg.Lock()
	if h.running { // ya esta corriendo
		h.mu_seg.Unlock()
		return fmt.Errorf("hlsplay: ALREADY_RUNNING_ERROR")
	}
	// FIFO debe existir previo al uso de este objeto en: fiforoot+"fifo" (mkfifo /var/segments/fifo)
	fw, err = os.OpenFile(fiforoot+"fifo", os.O_RDWR, os.ModeNamedPipe) // abrimos el named pipe fifo como +rw para evitar bloqueos
	if err != nil {
		Warning.Fatalln(err)
	}
	// borrar la base de datos de RAM y los ficheros *.ts
	exec.Command("/bin/sh", "-c", "rm -f "+h.downloaddir+"*.ts").Run() // equivale a rm -f /var/segments/*.ts
	h.running = true
	h.badfifo = false
	h.paused = false
	h.execpause = false // comienza a correr
	h.lastm3u8 = 0
	h.mu_seg.Unlock()

	go h.m3u8parser() // baja y parsea la .m3u8 para llenar la cola de bajadas
	go h.downloader() // bajando a su bola sin parar los segmentos .ts
	go h.director()   // envia segmentos al secuenciador y los saca por el FIFO para remux+playback

	return err
}
