# ChainTree Viewer — Continuation Roadmap

This document tracks planned development phases for evolving the viewer
from a static demo into a live ChainTree monitoring and editing tool,
then porting to Windows and iOS.

---

## Phase 1: Core Viewer Improvements (Linux) ✅ In Progress

### 1.1 ✅ Basic tree view with example data
- QTreeView with QStandardItemModel
- Node types: BT (Sequence, Selector, Parallel, Action, Condition, Decorator),
  SM (Machine, State, Transition), CF (Chain, Step, Event)
- Color-coded type and status columns
- Dark Fusion theme
- Expand/collapse toolbar
- Status bar with ltree path display

### 1.2 Context menus and node inspection
- Right-click context menu on tree nodes
- Node detail panel (dock widget or split view) showing:
  - Full ltree path
  - Node type and parameters
  - S-expression source (if available)
  - Runtime status history
- Copy path / copy S-expression to clipboard

### 1.3 Search and filter
- Filter bar to search nodes by name, type, path, or status
- QSortFilterProxyModel for live filtering without rebuilding the tree
- Highlight matching nodes with expand-to-match

### 1.4 Tree icons
- Small SVG or PNG icons per node type (distinct shapes for BT/SM/CF)
- Qt resource system (.qrc) for embedding icons in the binary
- Optional: status overlay badges on icons

---

## Phase 2: Custom Model and Data Loading

### 2.1 Replace QStandardItemModel with QAbstractItemModel
- Custom ChainTreeItemModel backed by an internal node structure
- Lazy child loading for large trees (fetchMore / canFetchMore)
- Better memory efficiency for trees with thousands of nodes

### 2.2 Load from YAML
- Parse ChainTree YAML definitions (the output of the Python DSL)
- File > Open dialog for .yaml / .yml files
- Drag-and-drop file loading
- Recent files list

### 2.3 Load from .ctb binary images
- Read .ctb files using the binary image format
  (header, function table, node descriptors)
- Display raw binary metadata alongside the tree structure
- Useful for verifying the LuaJIT emitter (stage6_binary.lua) output

### 2.4 Load from PostgreSQL knowledge base
- Connect to PostgreSQL and query ltree-structured knowledge base
- Use the ltree path column to reconstruct the tree hierarchy
- Connection settings dialog (host, port, database, credentials)
- Optional: libpq or Qt SQL module (QPSQL driver)

---

## Phase 3: Live Monitoring via NATS

### 3.1 NATS JetStream subscription
- Connect to NATS server and subscribe to ChainTree status topics
- Map incoming status messages to tree node updates
- Real-time color/status changes as nodes tick
- Connection indicator in toolbar or status bar

### 3.2 MQTT alternative path
- MQTT subscription for environments using MQTT instead of NATS
- Configurable broker URL and topic patterns
- Same status update pipeline as NATS

### 3.3 Status history and logging
- Ring buffer of recent status changes per node
- Scrollable log panel (dock widget) showing timestamped events
- Optional: highlight nodes that changed in the last N seconds
- Pause/resume live updates

### 3.4 Performance considerations
- Batch UI updates (don't repaint on every single NATS message)
- Coalesce rapid status flips
- Profile with large trees (1000+ nodes) under continuous update

---

## Phase 4: S-Expression Integration

### 4.1 Display S-expressions
- Show the S-expression DSL source for each node
- Syntax-highlighted read-only text panel (QSyntaxHighlighter or QScintilla)
- Map tree selection to S-expression source location

### 4.2 Avro packet inspection
- Decode avro_dsl.lua wire packets
- Display packet contents alongside the tree node that produced them
- Useful for debugging the S-Engine runtime

---

## Phase 5: Windows Port

### 5.1 Build system
- Verify CMakeLists.txt works with Qt 6 on Windows (MSVC or MinGW)
- Add CMake presets for Windows builds
- CI: GitHub Actions with windows-latest and Qt 6 from aqtinstall

### 5.2 Packaging
- Single executable via static Qt build (commercial license) or
  dynamically linked with Qt DLLs bundled via windeployqt
- NSIS or WiX installer, or simply a zip with the exe + DLLs
- Code signing (optional)

### 5.3 Platform-specific adjustments
- Test dark theme on Windows (Fusion style should work identically)
- Verify NATS/MQTT client libraries compile on Windows
- File path handling (forward vs back slashes in ltree paths)

---

## Phase 6: iOS Port

### 6.1 Build system
- Qt 6 for iOS toolchain (requires macOS + Xcode)
- CMake cross-compilation with Qt iOS platform
- Produces an .app bundle / .ipa

### 6.2 UI adaptation
- Touch-friendly tree interaction (larger tap targets)
- Consider QML overlay or hybrid Widgets+Quick approach
  for iOS-appropriate gestures (swipe to expand, pinch to zoom)
- Respect iOS safe areas and dynamic type sizes

### 6.3 Feature scoping for mobile
- Read-only viewer initially (no editing)
- NATS/MQTT live monitoring over WiFi
- Load trees from files or URLs (no local PostgreSQL)
- Share tree snapshots via iOS share sheet

### 6.4 App Store considerations
- Dynamic linking to Qt (LGPL compliance on iOS is harder —
  may need commercial Qt license for iOS distribution)
- App Store review guidelines compliance
- Privacy manifest (required since 2024)

---

## Phase 7: Advanced Features (Post-Port)

### 7.1 Tree editing
- Add / remove / reorder nodes via drag-and-drop
- Edit node properties inline or in a property panel
- Export modified tree back to YAML or S-expression DSL

### 7.2 Graphical tree layout (optional)
- QGraphicsView-based node graph as an alternative to the tree view
- Visual connections between state machine transitions
- Zoomable, pannable canvas for large trees

### 7.3 Multiple trees
- Tabbed interface for viewing multiple ChainTree instances
- Side-by-side diff of two tree versions
- Useful for comparing deployed vs development configurations

### 7.4 CFL runtime integration
- Embed the C runtime (cfl_image_loader) directly into the viewer
- Step through tree execution tick-by-tick
- Breakpoints on specific nodes
- Variable/register inspection

---

## Build & CI Notes

- **Dynamic linking** on all desktop platforms to stay LGPL-compliant
- **Static linking** on iOS likely requires Qt commercial license
- GitHub Actions CI matrix: Ubuntu (ARM64 + x86_64), Windows, macOS
- Consider CPack for cross-platform packaging
- Versioning: semantic versioning, tag-based releases

---

## Dependencies Tracker

| Dependency     | Purpose                  | License   | Phase |
|---------------|--------------------------|-----------|-------|
| Qt 6 Widgets  | UI framework             | LGPL v3   | 1     |
| Qt 6 Network  | NATS/MQTT TCP sockets    | LGPL v3   | 3     |
| Qt 6 SQL      | PostgreSQL access        | LGPL v3   | 2.4   |
| nats.c        | NATS client library      | Apache 2  | 3.1   |
| mosquitto     | MQTT client library      | EPL/EDL   | 3.2   |
| yaml-cpp      | YAML parsing             | MIT       | 2.2   |

---

*Last updated: March 2026*
