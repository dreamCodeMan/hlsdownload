package hlsdownload

import (
	//	"github.com/isaacml/cmdline"
	"github.com/todostreaming/cola"
	"github.com/todostreaming/m3u8pls"
	"log"
	"os"
	"sync"
	"fmt"
	"os/exec"
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
	// FIFO debe existir previo al uso de este objeto en: fiforoot+"fifo" (mkfifo /var/segments/fifo)
	var err error
	fw, err = os.OpenFile(fiforoot+"fifo", os.O_WRONLY|os.O_TRUNC, 0666) /// |os.O_CREATE|os.O_APPEND (O_WRONLY|O_CREAT|O_TRUNC)
	if err != nil {
		Warning.Fatalln(err)
	}

	Warning = log.New(os.Stderr, "\n\n[WARNING]: ", log.Ldate|log.Ltime|log.Lshortfile)
}

type HLSPlay struct {
	duration      []float64  // matrices de duracion en segundos de los segmentos play?.ts
	downloaddir   string     // directorio RAMdisk donde se guardan los ficheros bajados del server y listos para reproducir
	m3u8          string     // playlist HLS *.m3u8 a bajarse para playback
	downloading   bool       // esta bajando segmentos
	running       bool       // proceso completo funcionando
	mu_seg        sync.Mutex // Mutex para las variables internas del objeto HLSPlay
	numsegs       int
	lastTargetdur float64
	lastMediaseq  int64
	lastIndex     int              // index del segmento donde toca copiar download.ts  entre 0 y numsegs-1
	lastPlay      int              // index del segmento que se envió al secuenciador desde el director
	lastkbps      int              // download kbps speed
	m3u8pls       *m3u8pls.M3U8pls // parser M3U8
	cola          *cola.Cola       // cola con los segments/dur para bajar
	mu_play       []sync.Mutex     // Mutex para la escritura/lectura de segmentos *.ts cíclicos
}

func HLSPlayer(m3u8, downloaddir string, settings map[string]string) *HLSPlay {
	hls := &HLSPlay{}
	hls.mu_seg.Lock()
	defer hls.mu_seg.Unlock()
	hls.downloaddir = downloaddir
	hls.m3u8 = m3u8
	hls.downloading = false
	hls.running = false
	hls.lastTargetdur = 0.0
	hls.lastMediaseq = 0
	hls.lastIndex = 0
	hls.lastPlay = 0
	hls.lastkbps = 0
	hls.m3u8pls = m3u8pls.M3U8playlist(hls.m3u8)
	hls.cola = cola.CreateQueue(queuetimeout)
	// calculamos los segmentos máximos que caben
	hls.numsegs = 7                               // 7 segmentos cíclicos
	hls.mu_play = make([]sync.Mutex, hls.numsegs) // segmentos de maximo 12 segundos a 5 Mbps
	hls.duration = make([]float64, hls.numsegs)

	return hls
}

func (h *HLSPlay) Run() error {
	var err error

	h.mu_seg.Lock()
	if h.running { // ya esta corriendo
		h.mu_seg.Unlock()
		return fmt.Errorf("hlsplay: ALREADY_RUNNING_ERROR")
	}
	// borrar la base de datos de RAM y los ficheros *.ts
	exec.Command("/bin/sh", "-c", "rm -f "+h.downloaddir+"*.ts").Run() // equivale a rm -f /var/segments/*.ts
	h.running = true                                                   // comienza a correr
	h.mu_seg.Unlock()

//	go h.m3u8parser()
//	go h.downloader() // bajando a su bola sin parar
//	go h.director()   // envia segmentos al secuenciador cuando s.playing && s.restamping

	return err
}

func (h *HLSPlay) Stop() error {
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
	h.lastkbps = 0
	h.cola = cola.CreateQueue(queuetimeout)
	h.duration = make([]float64, h.numsegs)

	return err
}

