package hlsdownload

import (
	//	"github.com/isaacml/cmdline"
	"github.com/todostreaming/cola"
	"github.com/todostreaming/m3u8pls"
	"log"
	"os"
	"sync"
	"syscall"
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
	syscall.Mkfifo(fiforoot+"fifo", 0666)

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
