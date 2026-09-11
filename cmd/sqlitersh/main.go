package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	var startCtx string
	if len(os.Args) >= 2 {
		startCtx = os.Args[1]
	} else {
		startCtx = "file://."
	}

	p := tea.NewProgram(initialModel(startCtx), tea.WithAltScreen(), tea.WithMouseCellMotion())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Alas, there's been an error: %v\n", err)
		os.Exit(1)
	}
}
