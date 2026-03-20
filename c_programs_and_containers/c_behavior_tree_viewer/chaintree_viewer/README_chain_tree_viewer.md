# ChainTree Viewer

A Qt 6 Widgets desktop application for visualizing ChainTree hierarchies —
behavior trees, state machines, and sequential control flows rendered in a
single interactive tree view with color-coded node types and runtime status.

ChainTree is a unified control architecture that combines behavior trees,
state machines, and sequential control flows into a single framework,
targeting hardware from 32 KB ARM Cortex-M microcontrollers up to 8 GB+
servers. This viewer provides visual inspection and (eventually) live
monitoring of ChainTree instances.

## Screenshots

*(Coming soon — the app renders a dark-themed tree with blue/green/orange
color families for BT/SM/CF node types and status indicators.)*

## Features

- **QTreeView** with four columns: Node, Type, Status, Path
- **Color-coded node types**
  - Blue family — Behavior tree (Sequence, Selector, Parallel, Action, Condition, Decorator)
  - Green family — State machine (Machine, State, Transition)
  - Orange family — Control flow (Chain, Step, Event)
- **Status indicators** — Idle (gray), Running (cyan), Success (green), Failure (red)
- **ltree-style paths** — each node carries its hierarchical path (e.g. `irrigation.scheduler.zone_sel.zone1`)
- **Dark theme** — Fusion style with dark palette, easy on the eyes
- **Expand/Collapse toolbar** — quick navigation of deep trees
- **Status bar** — shows full node metadata on click

## Requirements

- Qt 6 (qt6-base-dev)
- CMake 3.16+
- C++17 compiler (g++ or clang++)

### Platform-specific packages

**Ubuntu / Debian:**
```bash
sudo apt install qt6-base-dev cmake g++
```

**Fedora:**
```bash
sudo dnf install qt6-qtbase-devel cmake gcc-c++
```

**Arch:**
```bash
sudo pacman -S qt6-base cmake
```

## Build

```bash
chmod +x build.sh
./build.sh
```

Or manually:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Run

```bash
./build/chaintree_viewer
```

### WSL2 Notes

If running under WSL2 with WSLg, you may need:

```bash
# Fix Wayland plugin failure
export QT_QPA_PLATFORM=xcb

# Fix runtime directory permissions warning
export XDG_RUNTIME_DIR=/tmp/runtime-$(id -u)
mkdir -p "$XDG_RUNTIME_DIR"
chmod 0700 "$XDG_RUNTIME_DIR"
```

Add both to `~/.bashrc` to make them persistent.

## Project Structure

```
chaintree_viewer/
├── CMakeLists.txt
├── build.sh
├── README.md
├── LICENSE
├── continue.md
└── src/
    ├── main.cpp                - Entry point, dark theme setup
    ├── mainwindow.h/.cpp       - Main window, QTreeView, toolbar, status bar
    └── chaintree_model.h/.cpp  - Tree model, node types, color coding
```

## Example Tree

The app loads a demonstration hierarchy modeled on a real irrigation
controller system:

```
Irrigation Controller (Root)
├── Zone Scheduler (BT:Sequence)
│   ├── Check Weather (BT:Condition)
│   ├── Check Soil Moisture (BT:Condition)
│   └── Zone Selector (BT:Selector)
│       ├── Zone 1 - Front Lawn (BT:Action)
│       ├── Zone 2 - Garden (BT:Action)
│       └── Zone 3 - Back Yard (BT:Action)
├── Valve Controller (SM:Machine)
│   ├── Closed (SM:State)
│   │   └── on_start -> Opening (SM:Transition)
│   ├── Opening (SM:State)
│   │   └── on_complete -> Open (SM:Transition)
│   ├── Open (SM:State)
│   │   └── on_timeout -> Closing (SM:Transition)
│   └── Closing (SM:State)
│       └── on_complete -> Closed (SM:Transition)
└── Startup Sequence (CF:Chain)
    ├── Initialize Hardware (CF:Step)
    ├── Load Configuration (CF:Step)
    ├── Connect NATS (CF:Step)
    └── Publish Online Event (CF:Event)
```

## Licensing

This project is released under the **GNU General Public License v3 (GPL v3)**.
See [LICENSE](LICENSE) for details.

Qt 6 is also available under the GPL v3, making this a fully GPL-compatible
stack with no additional linking or distribution requirements. See
[qt.io/licensing](https://www.qt.io/licensing/) for Qt's licensing options.

## Related Projects

- [knowledge_base_container](https://github.com/glenn-edgar/knowledge_base_container) — ChainTree and related projects (MIT)
- [knowledge_base](https://github.com/glenn-edgar/knowledge_base) — ltree knowledge base project (MIT)

## Author

Glenn Edgar — Onyx Engineering