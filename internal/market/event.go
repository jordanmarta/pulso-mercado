package market

import "time"

type QuoteEvent struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    int64     `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
}

func NewQuoteEvent(symbol string, price float64, volume int64) QuoteEvent {
	return QuoteEvent{
		Symbol:    symbol,
		Price:     price,
		Volume:    volume,
		Timestamp: time.Now().UTC(),
	}
}
