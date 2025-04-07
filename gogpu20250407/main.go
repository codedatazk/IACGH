package main
import ("fmt"; "runtime")
import "flag"
import "net/http"
import "github.com/prometheus/common/log"
import "github.com/prometheus/client_golang/prometheus"
import "github.com/prometheus/client_golang/prometheus/promhttp"

var listenAddress = flag.String(
	"listen-address",
	":8080",
	"The address to listen on for HTTP requests.")

func main() {
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN: %s file: %s, line: %d\n", runtime.FuncForPC(funcname).Name(), file, line)
    }
	flag.Parse()
	prometheus.MustRegister(GPUsCollectorRun())   // from gpus.go

	log.Infof("Starting Server: %s", *listenAddress)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
