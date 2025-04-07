package main

import "fmt"  
import "runtime"
import "os"
import "os/exec"
import "io"
import "io/ioutil"
import "strings"
import "strconv"
import "github.com/prometheus/common/log"
import "github.com/prometheus/client_golang/prometheus"

type QuotaGPUs struct {
	all       float64
	assign       float64
	idle        float64
	utiliz float64
}

type UserTime struct {
    jobid int

}

func AllocatedGPUsSacct() float64 {
    funcname2, file2, line2, okzk2 := runtime.Caller(0)
    if(okzk2){
        fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname2).Name(), file2, line2)
    }
	
    var num_gpus = 0.0

    cmdsacct := exec.Command("sacct", "-a", "-X", "--format=Allocgres", "--state=RUNNING", "--noheader", "--parsable2")
    stdoutsacct, errsacct := cmdsacct.StdoutPipe()
    if errsacct != nil{
        log.Fatal(errsacct)
    }

    if errsacct := cmdsacct.Start(); errsacct != nil{
        log.Fatal(errsacct)
    }

    outsacct, _ := ioutil.ReadAll(stdoutsacct)
    if errwait := cmdsacct.Wait(); errwait != nil{
        log.Fatal(errwait)
    }
	
    if len(outsacct) > 0 {
        funcname3, file3, line3, okzk3 := runtime.Caller(0)
        if(okzk3){
            fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname3).Name(), file3, line3, outsacct)
        }

		for _, line := range strings.Split(string(outsacct), "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"") 

				descriptor := strings.TrimPrefix(line, "gpu:") 
                
				job_gpus, _ := strconv.ParseFloat(descriptor, 64)
				num_gpus += job_gpus
			}
		}
	}
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d %f\n",runtime.FuncForPC(funcname).Name(), file, line, num_gpus)
    }

	return num_gpus
}

func TotalGPUsSinfo() float64 {
	var num_gpus = 0.0
    
    funcname2, file2, line2, okzk2 := runtime.Caller(0)
    if(okzk2){
        fmt.Printf("FN:%s File: %s, Line: %d\n", runtime.FuncForPC(funcname2).Name(), file2, line2)
    }
    
    cmdsinf := exec.Command("sinfo", "-h", "-o \"%n %G\"")
    stdoutsinf, errsinf := cmdsinf.StdoutPipe()
    if errsinf != nil{
        log.Fatal(errsinf)
    }
    if errsinf := cmdsinf.Start(); errsinf != nil{
        log.Fatal(errsinf)
    }

    outsinf, _ := ioutil.ReadAll(stdoutsinf)
    if errsinf3 := cmdsinf.Wait(); errsinf3 != nil{
        log.Fatal(errsinf3)
    }
   
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d %s %d\n",runtime.FuncForPC(funcname).Name(), file, line, outsinf, len(outsinf))
    }
    
    funcname3, file3, line3, okzk3 := runtime.Caller(0)
    if(okzk3){
        fmt.Printf("FN:%s File: %s, Line: %d %c %c %c %c\n",runtime.FuncForPC(funcname3).Name(), file3, line3, outsinf[0], outsinf[1], outsinf[2], outsinf[3])
    }
    
	if len(outsinf) > 0 {
		for _, line := range strings.Split(string(outsinf), "\n") {
			if len(line) > 0 {
				line = strings.Trim(line, "\"")
				descriptor := strings.Fields(line)[1]
				descriptor = strings.TrimPrefix(descriptor, "gpu:")
				descriptor = strings.Split(descriptor, "(")[0]
				node_gpus, _ :=  strconv.ParseFloat(descriptor, 64)
				num_gpus += node_gpus
			}
		}
	}

	return num_gpus
}

func fileExists(filename string) bool {
    info, err := os.Stat(filename)
    if os.IsNotExist(err) {
        return false
    }
    return !info.IsDir()
}

func ParseUserTime() *UserTime {
    //jobid
    cmd := exec.Command("sacct", "-a", "-X", "--state=RUNNING", "--noheader", "--format=jobid,Allocgres", "--parsable2")
    stdout, err := cmd.StdoutPipe()
    if err != nil{
        log.Fatal(err)
    }
    if err := cmd.Start(); err != nil{
        log.Fatal(err)
    }
    
    out, _ := ioutil.ReadAll(stdout)
    if err1 := cmd.Wait(); err1 != nil {
		log.Fatal(err1)
	}

    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname).Name(), file, line, out)
    }
    
    for _, linecont := range strings.Split(string(out), "\n") {
        if(strings.Contains(linecont, "gpu")){
            startpos := strings.Index(linecont, "gpu")
            jobid := linecont[0:(startpos-1)]
            funcname2, file2, line2, okzk2 := runtime.Caller(0)
            if(okzk2){
                fmt.Printf("FN:%s File:%s, Line:%d %s\n",runtime.FuncForPC(funcname2).Name(), file2, line2, jobid )
            }
            cmdcst := exec.Command("bash", "-c", "scontrol show job " + jobid + "|grep StartTime")
            stdoutcst, errcst := cmdcst.StdoutPipe()
            if errcst != nil{
                log.Fatal(errcst)
            }
            if errcst := cmdcst.Start(); errcst != nil{
                log.Fatal(errcst)
            }

            outcst, _ := ioutil.ReadAll(stdoutcst)
            if errcst := cmdcst.Wait(); errcst != nil {
		        log.Fatal(errcst)
	        }

            funcname3, file3, line3, okzk3 := runtime.Caller(0)
            if(okzk3){
                fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname3).Name(), file3, line3, outcst)
            }

            cmdsq := exec.Command("squeue","--job", jobid, "-a","-r","-h","-o %A|%u|%M")
            stdoutsq, errsq := cmdsq.StdoutPipe()
            if errsq != nil{
                log.Fatal(errsq)
            }
            if errsq := cmdsq.Start(); errsq != nil{
                log.Fatal(errsq)
            }

            outsq, _ := ioutil.ReadAll(stdoutsq)
            if errsq := cmdsq.Wait(); errsq != nil {
		        log.Fatal(errsq)
	        }

            funcname10, file10, line10, okzk10 := runtime.Caller(0)
            if(okzk10){
                fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname10).Name(), file10, line10, outsq)
            }
    
    
            fname := "./res" + jobid //fname, file name.
            var fobj *os.File
            var err2 error
            if fileExists(fname) {
                funcname4, file4, line4, okzk4 := runtime.Caller(0)
                if(okzk4){
                    fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname4).Name(), file4, line4)
                }

                fobj, err2 = os.OpenFile(fname, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
                if err2 != nil{
                    funcname5, file5, line5, okzk5 := runtime.Caller(0)
                    if(okzk5){
                        fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname5).Name(), file5, line5, err2)
                    }
                }
            }else {
                funcname7, file7, line7, okzk7 := runtime.Caller(0)
                if(okzk7){
                    fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname7).Name(), file7, line7)
                }
        
                fobj, err2 = os.Create(fname)
                if err2 != nil {
                    funcname6, file6, line6, okzk6 := runtime.Caller(0)
                    if(okzk6){
                        fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname6).Name(), file6, line6, err2)
                    }
                }
            }
            writen, err3 := io.WriteString(fobj, string(outsq) + "\n" + string(outcst))
            if err3 != nil {
                funcname8, file8, line8, okzk8 := runtime.Caller(0)
                if(okzk8){
                    fmt.Printf("FN:%s File: %s, Line: %d %s\n",runtime.FuncForPC(funcname8).Name(), file8, line8, err3)
                }
            }
            funcname9, file9, line9, okzk9 := runtime.Caller(0)
            if(okzk9){
                fmt.Printf("FN:%s File: %s, Line: %d WriteByteNumber:%d\n",runtime.FuncForPC(funcname9).Name(), file9, line9, writen)
            }

        }
    }

    var ut UserTime
    return &ut
}

func GainQuotaGPUs() *QuotaGPUs {
    funcname2, file2, line2, okzk2 := runtime.Caller(0)
    if(okzk2){
        fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname2).Name(), file2, line2)
    }
    
	var gpuq QuotaGPUs
	all_gpus := TotalGPUsSinfo()
    
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d %f\n",runtime.FuncForPC(funcname).Name(), file, line, all_gpus)
    }

	allocated_gpus := AllocatedGPUsSacct()
    
    if(allocated_gpus > 0){
        var utres *UserTime
        utres = ParseUserTime()
        funcname4, file4, line4, okzk4 := runtime.Caller(0)
        if(okzk4){
            fmt.Printf("FN:%s File: %s, Line: %d %d\n",runtime.FuncForPC(funcname4).Name(), file4, line4, utres.jobid)
        }
    }
    
    funcname3, file3, line3, okzk3 := runtime.Caller(0)
    if(okzk3){
        fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname3).Name(), file3, line3)
    }

	gpuq.all = all_gpus
    gpuq.assign = allocated_gpus
	gpuq.idle = all_gpus - allocated_gpus
	gpuq.utiliz = allocated_gpus / all_gpus
	return &gpuq
}

func GPUsCollectorRun() *GPUsCollector {
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d\n", runtime.FuncForPC(funcname).Name(), file, line)
    }
	return &GPUsCollector{
		all: prometheus.NewDesc("slurm_gpus_total", "All GPUs", nil, nil),
		allot: prometheus.NewDesc("slurm_gpus_alloc", "Allot GPUs", nil, nil),
		idle:  prometheus.NewDesc("slurm_gpus_idle", "Idle GPUs", nil, nil),
		utiliz: prometheus.NewDesc("slurm_gpus_utiliz", "All GPU utiliz", nil, nil),
	}
}

type GPUsCollector struct {
	all       *prometheus.Desc
	allot       *prometheus.Desc
	idle        *prometheus.Desc
	utiliz *prometheus.Desc
}

func (cc *GPUsCollector) Describe(ch chan<- *prometheus.Desc) {
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s, file: %s, line: %d %s\n", runtime.FuncForPC(funcname).Name(), file, line, cc.allot)
        fmt.Printf("File: %s, line: %d %s\n", file, line, cc.all)
        fmt.Printf("file: %s, Line: %d %s\n", file, line, cc.idle)
        fmt.Printf("File: %s, Line: %d %s\n", file, line, cc.utiliz)
    }
	ch <- cc.all
	ch <- cc.allot
	ch <- cc.idle
	ch <- cc.utiliz
}


func (cc *GPUsCollector) Collect(ch chan<- prometheus.Metric) {
    funcname, file, line, okzk := runtime.Caller(0)
    if(okzk){
        fmt.Printf("FN:%s File: %s, Line: %d\n",runtime.FuncForPC(funcname).Name(), file, line)
    }

    cm := GainQuotaGPUs()
	ch <- prometheus.MustNewConstMetric(cc.all, prometheus.GaugeValue, cm.all)
	ch <- prometheus.MustNewConstMetric(cc.allot, prometheus.GaugeValue, cm.assign)
	ch <- prometheus.MustNewConstMetric(cc.idle, prometheus.GaugeValue, cm.idle)
	ch <- prometheus.MustNewConstMetric(cc.utiliz, prometheus.GaugeValue, cm.utiliz)
}
