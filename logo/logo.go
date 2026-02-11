package logo

import (
	"fmt"
	"os"
)

type Logo struct {
	logo string
}

// Create the Logo
func NewLogo() *Logo {
	l := Logo{}
	l.readLogo()
	return &l
}

// read logo from file
func (l *Logo) readLogo() {
	b, err := os.ReadFile("logo.txt")

	// if we dont find any logo - die...
	if err != nil {
		panic(err)
	}
	l.logo = string(b)
}

// Shows a hydrakv logo at startup in ASCII Art
func (l *Logo) ShowLogo() {
	fmt.Println("HydraKV starting Up...")
	fmt.Println(l.logo)
	fmt.Println("")
}
