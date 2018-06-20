package tsdbctl

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"math"
	"strconv"
	"strings"
	"time"
	"sort"
	"encoding/csv"
	"bytes"
	"io/ioutil"
	"github.com/v3io/v3io-tsdb/pkg/tsdb"
)

const ARRAY_SEPARATOR = ";"

type addCommandeer struct {
	cmd            *cobra.Command
	rootCommandeer *RootCommandeer
	name           string
	lset           string
	tArr           string
	vArr           string
	inFile         string
}

func newAddCommandeer(rootCommandeer *RootCommandeer) *addCommandeer {
	commandeer := &addCommandeer{
		rootCommandeer: rootCommandeer,
	}

	cmd := &cobra.Command{
		Use:     "add metric [labels] [flags]",
		Aliases: []string{"append"},
		Short:   "add samples to metric. e.g. add http_req method=get -d 99.9",
		RunE: func(cmd *cobra.Command, args []string) error {

			if commandeer.inFile == "" {
				// if its not using an input CSV file check for name & labels arguments 
				if len(args) == 0 {
					return errors.New("add require metric name and/or labels")
				}

				commandeer.name = args[0]

				if len(args) > 1 {
					commandeer.lset = args[1]
				}
			}

			return commandeer.add()

		},
	}

	cmd.Flags().StringVarP(&commandeer.tArr, "times", "t", "", "time array, comma separated")
	cmd.Flags().StringVarP(&commandeer.vArr, "values", "d", "", "values array, comma separated")
	cmd.Flags().StringVarP(&commandeer.inFile, "file", "f", "", "CSV input file")

	commandeer.cmd = cmd

	return commandeer
}

func (ac *addCommandeer) add() error {

	var err error
	var lset utils.Labels

	// initialize params and adapter
	if err = ac.rootCommandeer.initialize(); err != nil {
		return err
	}

	if err = ac.rootCommandeer.startAdapter(); err != nil {
		return err
	}

	if ac.inFile == "" {
		// process direct CLI input
		if lset, err = strToLabels(ac.name, ac.lset); err != nil {
			return err
		}

		if ac.vArr == "" {
			return errors.New("must have at least one value")
		}

		tarray, varray, err := strToTV(ac.tArr, ac.vArr)
		if err != nil {
			return err
		}

		append, err := ac.rootCommandeer.adapter.Appender()
		if err != nil {
			return errors.Wrap(err, "failed to create Appender")
		}

		ref, err := ac.appendMetric(append, lset, tarray, varray)
		if err != nil {
			return err
		}

		return append.WaitForReady(ref)
	}

	// process a CSV file input
	data, err := ioutil.ReadFile(ac.inFile)
	if err != nil {
		errors.Wrap(err, "cant open/read CSV input file: " + ac.inFile)
	}

	r := csv.NewReader(bytes.NewReader(data))

	records, err := r.ReadAll()
	if err != nil {
		errors.Wrap(err, "cant read/process CSV input")
	}

	append, err := ac.rootCommandeer.adapter.Appender()
	if err != nil {
		return errors.Wrap(err, "failed to create Appender")
	}

	refMap := map[uint64]bool{}

	for num, line := range records {

		if len(line) < 3 || len(line) > 4  {
			return fmt.Errorf("must have 3-4 columns per row name,labels,value[,time] in line %d (%v)", num, line)
		}

		if lset, err = strToLabels(line[0], line[1]); err != nil {
			return err
		}

		tarr := ""
		if len(line)==4 {
			tarr = line[3]
		}

		tarray, varray, err := strToTV(tarr, line[2])
		if err != nil {
			return err
		}

		ref, err := ac.appendMetric(append, lset, tarray, varray)
		if err != nil {
			return err
		}

		refMap[ref] = true
	}

	// make sure all writes are committed
	return ac.waitForWrites(append, &refMap)
}

func (ac *addCommandeer) waitForWrites(append tsdb.Appender, refMap *map[uint64]bool) error {

	for ref, _ := range *refMap {
		err := append.WaitForReady(ref)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ac *addCommandeer) appendMetric(append tsdb.Appender, lset utils.Labels, tarray []int64, varray []float64) (uint64, error) {

	ac.rootCommandeer.logger.DebugWith("adding value to metric", "lset", lset, "t", tarray, "v", varray)

	fmt.Println("add:", lset, tarray, varray)
	ref, err := append.Add(lset, tarray[0], varray[0])
	if err != nil {
		return 0, errors.Wrap(err, "failed to Add")
	}

	for i := 1; i < len(varray); i++ {
		err := append.AddFast(lset, ref, tarray[i], varray[i])
		if err != nil {
			return 0, errors.Wrap(err, "failed to AddFast")
		}
	}

	return ref, nil
}


func strToLabels(name, lbls string) (utils.Labels, error) {

	lset := utils.Labels{utils.Label{Name:"__name__", Value: name}}

	if lbls != "" {
		splitLset := strings.Split(lbls, ",")
		for _, l := range splitLset {
			splitLbl := strings.Split(l, "=")
			if len(splitLbl) != 2 {
				return nil, errors.New("labels must be in the form: key1=label1,key2=label2,...")
			}
			lset = append(lset, utils.Label{Name:splitLbl[0], Value: splitLbl[1]})
		}
	}
	sort.Sort(lset)
	return lset, nil
}

func strToTV(tarr, varr string) ([]int64, []float64, error) {

	tlist := strings.Split(tarr, ARRAY_SEPARATOR)
	vlist := strings.Split(varr, ARRAY_SEPARATOR)

	if tarr == "" && len(vlist) > 1 {
		return nil, nil, errors.New("time array must be provided when using a value array")
	}

	if tarr != "" && len(tlist) != len(vlist) {
		return nil, nil, errors.New("time and value arrays must have the same number of elements")
	}

	tarray := []int64{}
	varray := []float64{}

	for i := 0; i < len(vlist); i++ {

		var v float64
		var err error
		if vlist[i] == "NaN" {
			v = math.NaN()
		} else {
			v, err = strconv.ParseFloat(vlist[i], 64)
			if err != nil {
				return nil, nil, errors.Wrap(err, "not a valid float value")
			}
		}

		varray = append(varray, v)
	}

	now := int64(time.Now().Unix() * 1000)
	if tarr == "" {
		tarray = append(tarray, now)
	} else {
		for i := 0; i < len(vlist); i++ {
			tstr := strings.TrimSpace(tlist[i])
			if tstr == "now" || tstr == "now-" {
				tarray = append(tarray, now)
			} else if strings.HasPrefix(tstr, "now-")  {
				t, err := str2duration(tstr[4:])
				if err != nil {
					return nil, nil, errors.Wrap(err, "not a valid time 'now-??', 'now' need to follow with nn[s|h|m|d]")
				}
				tarray = append(tarray, now - int64(t))
			} else {
				t, err := strconv.Atoi(tlist[i])
				if err != nil {
					return nil, nil, errors.Wrap(err, "not a valid (unix mili) time")
				}
				tarray = append(tarray, int64(t))
			}
		}
	}

	return tarray, varray, nil
}