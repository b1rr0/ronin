---
layout: page
title: PackMe - Custom File Archiver
permalink: /packme/
---

<style>
/* Code styling - Terminal look */
pre {
  background-color: #1e1e1e !important;
  color: #ffffff !important;
  border: 2px solid #333 !important;
  border-radius: 8px !important;
  padding: 20px !important;
  margin: 20px 0 !important;
  overflow-x: auto !important;
  max-width: 100% !important;
  font-family: 'Courier New', Consolas, Monaco, monospace !important;
  font-size: 14px !important;
  line-height: 1.4 !important;
  box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
}

pre code {
  background-color: transparent !important;
  color: #ffffff !important;
  padding: 0 !important;
  border: none !important;
  word-wrap: break-word !important;
  overflow-wrap: break-word !important;
}

/* Inline code styling */
code {
  background-color: #2d2d2d !important;
  color: #ffffff !important;
  padding: 2px 6px !important;
  border-radius: 4px !important;
  font-family: 'Courier New', Consolas, Monaco, monospace !important;
  font-size: 13px !important;
  word-wrap: break-word;
  overflow-wrap: break-word;
}

/* Syntax highlighting for Go code */
pre code .keyword { color: #569cd6 !important; }
pre code .string { color: #ce9178 !important; }
pre code .comment { color: #6a9955 !important; }
pre code .function { color: #dcdcaa !important; }
pre code .type { color: #4ec9b0 !important; }
</style>

# PackMe - Custom File Archiver

PackMe is a custom file archiver developed in Go using the Wails framework, designed to compress files into a proprietary `.PackMe` format.

## 🔧 Key Features

- **Custom .PackMe Format**: Proprietary compression format with advanced algorithms
- **Huffman Coding**: Implements entropy-based compression for optimal file size reduction
- **Z-function Algorithm**: Uses pattern detection for compression optimization
- **Cross-platform**: Built with Wails framework for desktop applications
- **Full Cycle Support**: Both compression and decompression capabilities

## 🎯 Technical Implementation

The project demonstrates several advanced computer science concepts:

- **Binary Trees**: Huffman tree construction for optimal encoding
- **Data Structures**: Efficient handling of file data and compression metadata
- **Pattern Recognition**: Z-function implementation for redundancy detection
- **File I/O**: Custom binary format handling and file system operations

## 🚀 Technologies Used

- **Language**: Go (Golang)
- **Framework**: Wails (for desktop GUI)
- **Algorithms**: Huffman coding, Z-function
- **Architecture**: Event-driven with clean separation of concerns

## 📚 Educational Value

This project serves as an excellent learning resource for:
- Understanding compression algorithms
- Exploring data structures like binary trees
- Learning about file format design
- Practicing Go programming with GUI frameworks

## 🔗 Source Code

Want to explore the implementation details or contribute to the project?

**[View on GitHub →](https://github.com/b1rr0/PackMe){:target="_blank"}**

The complete source code, documentation, and examples are available in the repository.