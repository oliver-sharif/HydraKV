package hashMap

type Basket struct {
	Items *Entry
}

// NewBasket returns a new Basket
func NewBasket() *Basket {
	return &Basket{}
}
