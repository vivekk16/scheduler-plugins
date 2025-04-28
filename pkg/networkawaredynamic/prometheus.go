package networkawaredynamic

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type PrometheusHandle struct {
	address            string
	port               string
	api                v1.API
	timeRange          string
	weightLatency      float64
	weightForwardBytes float64
	weightDropBytes    float64
}

const (
	forwardBytesQueryTemplate = "sum(rate(cilium_forward_bytes_total{instance=\"%s:%s\"}[%s]))/quantile_over_time(0.90,sum(rate(cilium_forward_bytes_total{instance=\"%s:%s\"}[%s]))[24h:])"
	dropBytesQueryTemplate    = "sum(rate(cilium_drop_bytes_total{instance=\"%s:%s\"}[%s]))/quantile_over_time(0.90,sum(rate(cilium_drop_bytes_total{instance=\"%s:%s\"}[%s]))[24h:])"
	latencyQueryTemplate      = "cilium_node_connectivity_latency_seconds{type=\"node\", instance=\"%s:%s\", target_node_ip=\"%s\", protocol=\"http\"}/quantile_over_time(0.90, cilium_node_connectivity_latency_seconds{type=\"node\", instance=\"%s:%s\", target_node_ip=\"%s\", protocol=\"http\"}[24h])"
)

// NewPrometheus initializes a new PrometheusHandle with the given address, port, and time range
func NewPrometheus(address, port, timeRange, weightLatency, weightForwardBytes, weightDropBytes string) *PrometheusHandle {
	client, err := api.NewClient(api.Config{Address: address})
	if err != nil {

		fmt.Errorf("error creating prometheus client: %s", err)
		return nil
	}

	// Parse weight values from string to float64
	latencyWeight, err := strconv.ParseFloat(weightLatency, 64)
	if err != nil {

		fmt.Errorf("error parsing weightLatency: %s", err)
		return nil
	}

	forwardBytesWeight, err := strconv.ParseFloat(weightForwardBytes, 64)
	if err != nil {

		fmt.Errorf("error parsing weightForwardBytes: %s", err)
		return nil
	}

	dropBytesWeight, err := strconv.ParseFloat(weightDropBytes, 64)
	if err != nil {

		fmt.Errorf("error parsing weightDropBytes: %s", err)
		return nil
	}

	return &PrometheusHandle{
		address:            address,
		api:                v1.NewAPI(client),
		timeRange:          timeRange,
		port:               port,
		weightLatency:      latencyWeight,
		weightForwardBytes: forwardBytesWeight,
		weightDropBytes:    dropBytesWeight,
	}
}

// Query executes a given Prometheus query and returns the result
func (p *PrometheusHandle) Query(query string) (model.Value, error) {
	val, warnings, err := p.api.Query(context.Background(), query, time.Now())
	if len(warnings) > 0 {
		fmt.Printf("Warnings: %s\n", warnings)
	}
	if err != nil {
		return nil, fmt.Errorf("error querying prometheus: %s", err)
	}
	return val, nil
}

// GetForwardBytesRatio calculates the forward bytes ratio based on the forward bytes query template
func (p *PrometheusHandle) GetForwardBytesRatio(instance string) (float64, error) {
	query := fmt.Sprintf(forwardBytesQueryTemplate, instance, p.port, p.timeRange, instance, p.port, p.timeRange)
	val, err := p.Query(query)
	if err != nil {
		return 0, err
	}

	// Extract the numeric value
	if vec, ok := val.(model.Vector); ok && len(vec) > 0 {
		return float64(vec[0].Value), nil
	}
	return 0, fmt.Errorf("no data for forward bytes ratio")
}

// GetDropBytesRatio calculates the drop bytes ratio based on the drop bytes query template
func (p *PrometheusHandle) GetDropBytesRatio(instance string) (float64, error) {
	query := fmt.Sprintf(dropBytesQueryTemplate, instance, p.port, p.timeRange, instance, p.port, p.timeRange)
	val, err := p.Query(query)
	if err != nil {
		return 0, err
	}

	// Extract the numeric value
	if vec, ok := val.(model.Vector); ok && len(vec) > 0 {
		return float64(vec[0].Value), nil
	}
	return 0, fmt.Errorf("no data for drop bytes ratio")
}

// GetLatencyRatio calculates the latency ratio based on the latency query template
func (p *PrometheusHandle) GetLatencyRatio(instance, targetNodeIP string) (float64, error) {
	query := fmt.Sprintf(latencyQueryTemplate, instance, p.port, targetNodeIP, instance, p.port, targetNodeIP)
	val, err := p.Query(query)
	if err != nil {
		return 0, err
	}

	// Extract the numeric value
	if vec, ok := val.(model.Vector); ok && len(vec) > 0 {
		return float64(vec[0].Value), nil
	}
	return 0, fmt.Errorf("no data for latency ratio")
}

// CalculateNetworkCost calculates the network cost based on the provided weights and ratios
func (p *PrometheusHandle) CalculateNetworkCost(instance, targetNodeIP string) (float64, error) {

	// Get ratios
	latency, err := p.GetLatencyRatio(instance, targetNodeIP)
	if err != nil {
		return 0, fmt.Errorf("error getting latency ratio: %s", err)
	}

	forwardBytes, err := p.GetForwardBytesRatio(instance)
	if err != nil {
		return 0, fmt.Errorf("error getting forward bytes ratio: %s", err)
	}

	dropBytes, err := p.GetDropBytesRatio(instance)
	if err != nil {
		return 0, fmt.Errorf("error getting drop bytes ratio: %s", err)
	}
	if math.IsNaN(dropBytes) || math.IsInf(dropBytes, 0) {
		dropBytes = 0
	}

	fmt.Println("Latency: ", latency)
	fmt.Println("Forward Bytes: ", forwardBytes)
	fmt.Println("Drop Bytes: ", dropBytes)

	// Calculate network cost
	networkCost := ((p.weightLatency * latency) +
		(p.weightForwardBytes * ((1 - forwardBytes)) +
		(p.weightDropBytes * dropBytes)) * 100

	return networkCost, nil
}
