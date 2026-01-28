<h1 align="center">Go DBMS</h1>
<p align="center">
<pre>
                             ██████   ██████     ██████  ██████  ███    ███ ███████
                            ██       ██    ██    ██   ██ ██   ██ ████  ████ ██
                            ██   ███ ██    ██    ██   ██ ██████  ██ ████ ██ ███████
                            ██    ██ ██    ██    ██   ██ ██   ██ ██  ██  ██      ██
                             ██████   ██████     ██████  ██████  ██      ██ ███████
</pre>
</p>

<p align="center">
  <em>⚡ A database management system built in Go from scratch ⚡</em>
</p>

<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)](https://golang.org)
[![Stars](https://img.shields.io/github/stars/spaghetti-lover/go-dbms?style=social)](https://github.com/spaghetti-lover/go-dbms/stargazers)

</div>

<a name="table-of-contents"></a>

## Table of contents

- [Table of contents](#table-of-contents)
- [Description](#description)
- [Installation](#installation)
  - [Requirements](#requirements)
  - [Clone the project](#clone-the-project)
  - [Usage](#usage)
- [Features](#features)
- [TODO](#todo)
- [License](#license)

<a name="description"></a>

## Description

This project is a database management system (DBMS) implemented in Go from scratch for learning purpose

- CLI:
<img src="docs/demo.gif" alt="Screenshot of demo" />
<!-- - UI:
  <img src="docs/ui.png" alt="Screenshot of UI" /> -->

<a name="installation"></a>

## Installation

<a name="requirements"></a>

### Requirements

- Go 1.24+

<a name="clone-the-project"></a>

### Clone the project

```bash
git clone https://github.com/spaghetti-lover/go-dbms
```

<a name="usage"></a>

## Usage

### Run the project

- Run CLI

```bash
go mod tidy
go run cmd/cli/main.go
```

- Benchmark

```bash
go mod tidy
go run cmd/benchmark/main.go
```

<a name="features"></a>

## Features

### Core Storage

- [x] B+ Tree (in-memory & disk-based)
- [x] Page layout (Header, Internal, Leaf)
- [x] Buffer Pool
- [x] WAL (redo-only)

### Data Model
- [x] Schema & record layout
- [x] Primary index
- [x] Secondary index
- [x] Range scan

### Query Layer
- [x] Table operations (INSERT / DELETE / UPDATE / SELECT)
- [x] Simple SQL parser
- [x] REPL


<a name="todo"></a>

## TODO
- [ ] Extended REPL
- [ ] Extended SQL grammar
- [ ] Transaction & Concurrency control

<a name="license"></a>

## License

[MIT](https://choosealicense.com/licenses/mit/)
