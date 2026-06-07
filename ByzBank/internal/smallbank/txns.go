package smallbank

import (
	"fmt"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// Kind identifies a SmallBank transaction type.
type Kind int

const (
	KindBalance Kind = iota
	KindDepositChecking
	KindTransactSavings
	KindAmalgamate
	KindWriteCheck
	KindSendPayment
)

func (k Kind) String() string {
	switch k {
	case KindBalance:
		return "Bal"
	case KindDepositChecking:
		return "DC"
	case KindTransactSavings:
		return "TS"
	case KindAmalgamate:
		return "Amg"
	case KindWriteCheck:
		return "WC"
	case KindSendPayment:
		return "SP"
	default:
		return "Unknown"
	}
}

// Op is one logical SmallBank operation.
type Op struct {
	Kind       Kind
	Cust       int
	Cust2      int
	Amt        int64
	ReadOnly   bool
	Penalty    bool
	Requests   []pbft.Request
	CrossShard bool
}

// Generator builds SmallBank operations.
type Generator struct {
	Schema   Schema
	ClientID string
	seq      int64
}

// NewGenerator constructs a txn generator.
func NewGenerator(schema Schema, clientID string) *Generator {
	return &Generator{Schema: schema, ClientID: clientID}
}

func (g *Generator) nextReq(x, y int, amt int64) pbft.Request {
	g.seq++
	return pbft.Request{
		ClientID: g.ClientID,
		TS:       g.seq,
		X:        x,
		Y:        y,
		Amt:      amt,
	}
}

// DepositChecking credits a customer's checking from the cluster treasury.
func (g *Generator) DepositChecking(cust int, amt int64) (Op, error) {
	if err := g.Schema.Validate(cust); err != nil {
		return Op{}, err
	}
	cluster := g.Schema.CustCluster(cust)
	chk := g.Schema.CheckingItem(cust)
	treasury := g.Schema.TreasuryItem(cluster)
	req := g.nextReq(treasury, chk, amt)
	return Op{
		Kind:     KindDepositChecking,
		Cust:     cust,
		Amt:      amt,
		Requests: []pbft.Request{req},
	}, nil
}

// TransactSavings adds amt to savings (negative amt withdraws to treasury).
func (g *Generator) TransactSavings(cust int, amt int64) (Op, error) {
	if err := g.Schema.Validate(cust); err != nil {
		return Op{}, err
	}
	cluster := g.Schema.CustCluster(cust)
	sav := g.Schema.SavingsItem(cust)
	treasury := g.Schema.TreasuryItem(cluster)
	var req pbft.Request
	if amt >= 0 {
		req = g.nextReq(treasury, sav, amt)
	} else {
		req = g.nextReq(sav, treasury, -amt)
	}
	return Op{
		Kind:     KindTransactSavings,
		Cust:     cust,
		Amt:      amt,
		Requests: []pbft.Request{req},
	}, nil
}

// SendPayment transfers between checking accounts.
func (g *Generator) SendPayment(cust1, cust2 int, amt int64) (Op, error) {
	if err := g.Schema.Validate(cust1); err != nil {
		return Op{}, err
	}
	if err := g.Schema.Validate(cust2); err != nil {
		return Op{}, err
	}
	x := g.Schema.CheckingItem(cust1)
	y := g.Schema.CheckingItem(cust2)
	req := g.nextReq(x, y, amt)
	return Op{
		Kind:       KindSendPayment,
		Cust:       cust1,
		Cust2:      cust2,
		Amt:        amt,
		Requests:   []pbft.Request{req},
		CrossShard: !g.Schema.Topo.SameCluster(x, y),
	}, nil
}

// WriteCheck debits checking; Penalty marks a penalty if funds are insufficient.
func (g *Generator) WriteCheck(cust int, amt int64, sufficient bool) (Op, error) {
	if err := g.Schema.Validate(cust); err != nil {
		return Op{}, err
	}
	cluster := g.Schema.CustCluster(cust)
	chk := g.Schema.CheckingItem(cust)
	treasury := g.Schema.TreasuryItem(cluster)
	if sufficient {
		req := g.nextReq(chk, treasury, amt)
		return Op{
			Kind:     KindWriteCheck,
			Cust:     cust,
			Amt:      amt,
			Requests: []pbft.Request{req},
		}, nil
	}
	// Insufficient: move available checking to treasury and record penalty.
	req := g.nextReq(chk, treasury, amt)
	return Op{
		Kind:     KindWriteCheck,
		Cust:     cust,
		Amt:      amt,
		Penalty:  true,
		Requests: []pbft.Request{req},
	}, nil
}

// Amalgamate moves savingsAmt from cust1 savings and checkAmt from cust1 checking into cust2 checking.
func (g *Generator) Amalgamate(cust1, cust2 int, savingsAmt, checkAmt int64) (Op, error) {
	if err := g.Schema.Validate(cust1); err != nil {
		return Op{}, err
	}
	if err := g.Schema.Validate(cust2); err != nil {
		return Op{}, err
	}
	reqs := make([]pbft.Request, 0, 2)
	cross := false
	if savingsAmt > 0 {
		x := g.Schema.SavingsItem(cust1)
		y := g.Schema.CheckingItem(cust2)
		reqs = append(reqs, g.nextReq(x, y, savingsAmt))
		cross = cross || !g.Schema.Topo.SameCluster(x, y)
	}
	if checkAmt > 0 {
		x := g.Schema.CheckingItem(cust1)
		y := g.Schema.CheckingItem(cust2)
		reqs = append(reqs, g.nextReq(x, y, checkAmt))
		cross = cross || !g.Schema.Topo.SameCluster(x, y)
	}
	if len(reqs) == 0 {
		return Op{}, fmt.Errorf("amalgamate %d->%d: zero balances", cust1, cust2)
	}
	return Op{
		Kind:       KindAmalgamate,
		Cust:       cust1,
		Cust2:      cust2,
		Requests:   reqs,
		CrossShard: cross,
	}, nil
}

// Balance is a read-only balance probe (no PBFT transfer).
func (g *Generator) Balance(cust int) (Op, error) {
	if err := g.Schema.Validate(cust); err != nil {
		return Op{}, err
	}
	return Op{
		Kind:     KindBalance,
		Cust:     cust,
		ReadOnly: true,
	}, nil
}

// RandomOp builds one operation of the given kind using the picker for customer selection.
func (g *Generator) RandomOp(kind Kind, picker *Picker, amt int64) (Op, error) {
	switch kind {
	case KindBalance:
		return g.Balance(picker.Pick())
	case KindDepositChecking:
		return g.DepositChecking(picker.Pick(), amt)
	case KindTransactSavings:
		cust := picker.Pick()
		if picker.Intn(2) == 0 {
			return g.TransactSavings(cust, amt)
		}
		return g.TransactSavings(cust, -amt/2)
	case KindSendPayment:
		c1 := picker.Pick()
		c2 := picker.Pick()
		for c2 == c1 {
			c2 = picker.Pick()
		}
		return g.SendPayment(c1, c2, amt)
	case KindWriteCheck:
		cust := picker.Pick()
		return g.WriteCheck(cust, amt, true)
	case KindAmalgamate:
		c1 := picker.Pick()
		c2 := picker.Pick()
		for c2 == c1 {
			c2 = picker.Pick()
		}
		return g.Amalgamate(c1, c2, amt, amt/2)
	default:
		return Op{}, fmt.Errorf("unknown kind %v", kind)
	}
}

// UniformKinds returns the six txn types in standard mix order.
func UniformKinds() []Kind {
	return []Kind{
		KindBalance,
		KindDepositChecking,
		KindTransactSavings,
		KindAmalgamate,
		KindWriteCheck,
		KindSendPayment,
	}
}
