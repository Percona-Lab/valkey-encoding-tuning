package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/caio/go-tdigest/v5"
)

type ListNodeType int

type listpackLimit struct {
	nodeDivisionType ListNodeType
	maxNodeSize      int64
}

const (
	listMaxListpackSize = "list-max-listpack-size"
	listCompressDepth   = "list-compress-depth"
	listDt              = "list"
)
const (
	Unknown ListNodeType = iota
	BySize
	ByElement
)

// since >1 node converts List to quicklist (still function as listpack as head & tail is uncompressed), we don't need total nodeCount
type ListMetrics struct {
	// for obj size or element count distribution, depending on the setting of `list-max-listpack-size` (positive or negative number)
	// for obj size distribution (default), affecting the number of nodes
	// for element count distribution, affecting the number of nodes if `list-max-listpack-size` is positive number
	nodeDivisionType ListNodeType
	tdigest          *tdigest.TDigest
	objCnt           int64
	avgNodeCnt       int64
	maxNodeCnt       int64
	avgObjSize       int64
	maxObjSize       int64
	avgElementCnt    int64
	maxElementCnt    int64
}

var optimizationLevel = map[string]int{
	"-1": 4 * 1024,
	"-2": 8 * 1024,
	"-3": 16 * 1024,
	"-4": 32 * 1024,
	"-5": 64 * 1024,
}

func makeListMetrics() ListMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return ListMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeList() error {
	limit, err := parseListpackLimit(v.Config[listMaxListpackSize])
	if err != nil {
		return err
	}
	v.ListMetrics.nodeDivisionType = limit.nodeDivisionType

	return v.analyze(listDt, nil, v.analyzeListKey)
}

func (v *ValkeyNode) analyzeListKey(key string) error {
	limit, err := parseListpackLimit(v.Config[listMaxListpackSize])
	if err != nil {
		return err
	}

	lm := &v.ListMetrics
	oldCount := lm.objCnt
	lm.objCnt++

	ctx := context.Background()
	client := v.getClient()
	// count the number of elements
	// whatever the value of `list-compress-depth` is, there will always be at least 2 nodes stored as uncompressed
	// if there are >= 3 nodes and low op/s, then it can be compressed, level 1
	// if there are more nodes, high op/s then can set compression to higher. Leave more nodes uncompressed
	// so need the element count to check if compression is suitable
	count, err := client.Do(
		ctx,
		client.B().Llen().Key(key).Build(),
	).AsInt64()
	if err != nil {
		return err
	}
	// get key size in bytes
	ksize, err := client.Do(
		ctx,
		client.B().MemoryUsage().Key(key).Build(),
	).AsInt64()

	if err != nil {
		return err
	}
	nodeCount := estimateListNodeCountFromLimit(limit, count, ksize)
	if limit.nodeDivisionType == BySize {
		lm.tdigest.Add(float64(ksize))
	} else {
		lm.tdigest.Add(float64(count))
	}
	lm.maxNodeCnt = max(lm.maxNodeCnt, nodeCount)
	lm.avgNodeCnt = (nodeCount + (lm.avgNodeCnt * oldCount)) / lm.objCnt

	lm.maxObjSize = max(lm.maxObjSize, ksize)
	lm.avgObjSize = (ksize + lm.avgObjSize*oldCount) / lm.objCnt

	lm.maxElementCnt = max(lm.maxElementCnt, count)
	lm.avgElementCnt = (count + lm.avgElementCnt*oldCount) / lm.objCnt

	return nil
}

func parseListpackLimit(configValue string) (listpackLimit, error) {
	if strings.HasPrefix(configValue, "-") {
		maxNodeSize, ok := optimizationLevel[configValue]
		if !ok {
			return listpackLimit{}, fmt.Errorf("unsupported %s optimization level %q", listMaxListpackSize, configValue)
		}
		if maxNodeSize <= 0 {
			return listpackLimit{}, fmt.Errorf("%s optimization level %q must be positive", listMaxListpackSize, configValue)
		}
		return listpackLimit{
			nodeDivisionType: BySize,
			maxNodeSize:      int64(maxNodeSize),
		}, nil
	}

	maxElementCount, err := strconv.Atoi(configValue)
	if err != nil {
		return listpackLimit{}, err
	}
	if maxElementCount <= 0 {
		return listpackLimit{}, errors.New("positive list-max-listpack-size must be greater than zero")
	}
	return listpackLimit{
		nodeDivisionType: ByElement,
		maxNodeSize:      int64(maxElementCount),
	}, nil
}

func estimateListNodeCount(configValue string, elementCount, objectSize int64) (int64, error) {
	limit, err := parseListpackLimit(configValue)
	if err != nil {
		return -1, err
	}
	return estimateListNodeCountFromLimit(limit, elementCount, objectSize), nil
}

func estimateListNodeCountFromLimit(limit listpackLimit, elementCount, objectSize int64) int64 {
	var elementSum int64
	if limit.nodeDivisionType == BySize {
		elementSum = objectSize
	} else {
		elementSum = elementCount
	}
	return int64(math.Ceil(float64(elementSum) / float64(limit.maxNodeSize)))
}

func (v *ValkeyNode) getListDatatypeAnalysis(analysis *Analysis) {
	var sizeDistrType string
	lm := v.ListMetrics
	if lm.nodeDivisionType == BySize {
		sizeDistrType = "element size"
	} else {
		sizeDistrType = "element count"
	}

	analysis.init(v.Address)
	analysis.Config[listMaxListpackSize] = v.Config[listMaxListpackSize]
	analysis.Config[listCompressDepth] = v.Config[listCompressDepth]

	analysis.Metrics[listDt] = map[string]any{
		kObjCnt:        lm.objCnt,
		kMaxNodeCnt:    lm.maxNodeCnt,
		kAvgNodeCnt:    lm.avgNodeCnt,
		kMaxElementCnt: lm.maxElementCnt,
		kAvgElementCnt: lm.avgElementCnt,
		kSizeDistrType: sizeDistrType,
		kDistribution:  quantileDistribution(lm.tdigest),
	}
}
func (lm *ListMetrics) updateListStatistics(node *ListMetrics) {
	if lm.nodeDivisionType != node.nodeDivisionType {
		if lm.nodeDivisionType != Unknown {
			//	do nothing due to incompatible node division type
			fmt.Fprintln(os.Stderr, "target node's listpack config is incompatible with cluster's config")
			return
		}
		// init value
		lm.nodeDivisionType = node.nodeDivisionType
	}
	objCount := (lm.objCnt + node.objCnt)
	if objCount == 0 {
		return
	}
	lm.avgNodeCnt = (lm.avgNodeCnt*lm.objCnt + node.avgNodeCnt*node.objCnt) / objCount
	lm.avgObjSize = (lm.avgObjSize*lm.objCnt + node.avgObjSize*node.objCnt) / objCount
	lm.avgElementCnt = (lm.avgElementCnt*lm.objCnt + node.avgElementCnt*node.objCnt) / objCount
	lm.objCnt = objCount
	lm.maxNodeCnt = max(lm.maxNodeCnt, node.maxNodeCnt)
	lm.maxObjSize = max(lm.maxObjSize, node.maxObjSize)
	lm.maxElementCnt = max(lm.maxElementCnt, node.maxElementCnt)
	lm.tdigest.Merge(node.tdigest)
}
