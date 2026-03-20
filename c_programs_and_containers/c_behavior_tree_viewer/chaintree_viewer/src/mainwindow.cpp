#include "mainwindow.h"

#include <QToolBar>
#include <QStatusBar>
#include <QHeaderView>
#include <QVBoxLayout>
#include <QAction>

MainWindow::MainWindow(QWidget *parent)
    : QMainWindow(parent)
{
    setWindowTitle("ChainTree Viewer");
    resize(900, 600);

    // --- Model ---
    m_model = new ChainTreeModel(this);
    m_model->loadExampleTree();

    // --- Tree View ---
    m_treeView = new QTreeView(this);
    m_treeView->setModel(m_model);
    m_treeView->setAlternatingRowColors(true);
    m_treeView->setAnimated(true);
    m_treeView->setSortingEnabled(true);
    m_treeView->setSelectionBehavior(QAbstractItemView::SelectRows);
    m_treeView->setSelectionMode(QAbstractItemView::SingleSelection);

    // Column widths
    m_treeView->header()->setStretchLastSection(true);
    m_treeView->setColumnWidth(0, 280);  // Node name
    m_treeView->setColumnWidth(1, 120);  // Type
    m_treeView->setColumnWidth(2, 80);   // Status

    // Expand top-level nodes by default
    m_treeView->expandToDepth(1);

    setCentralWidget(m_treeView);

    // --- Toolbar ---
    auto *toolbar = addToolBar("Navigation");
    toolbar->setMovable(false);

    auto *expandAll = new QAction("Expand All", this);
    connect(expandAll, &QAction::triggered, this, &MainWindow::onExpandAll);
    toolbar->addAction(expandAll);

    auto *collapseAll = new QAction("Collapse All", this);
    connect(collapseAll, &QAction::triggered, this, &MainWindow::onCollapseAll);
    toolbar->addAction(collapseAll);

    // --- Status Bar ---
    m_statusLabel = new QLabel("Click a node to see its ltree path");
    statusBar()->addWidget(m_statusLabel);

    // --- Connections ---
    connect(m_treeView, &QTreeView::clicked,
            this, &MainWindow::onNodeClicked);
}

void MainWindow::onNodeClicked(const QModelIndex &index)
{
    // Get the first-column index (where metadata is stored)
    QModelIndex nameIndex = index.siblingAtColumn(0);
    QString path = nameIndex.data(NodePathRole).toString();
    QString name = nameIndex.data(Qt::DisplayRole).toString();
    int typeInt  = nameIndex.data(NodeTypeRole).toInt();
    int statInt  = nameIndex.data(NodeStatusRole).toInt();

    auto type   = static_cast<NodeType>(typeInt);
    auto status = static_cast<NodeStatus>(statInt);

    m_statusLabel->setText(
        QString("  %1  |  %2  |  %3  |  path: %4")
            .arg(name)
            .arg(ChainTreeModel::nodeTypeName(type))
            .arg(ChainTreeModel::statusName(status))
            .arg(path)
    );
}

void MainWindow::onExpandAll()
{
    m_treeView->expandAll();
}

void MainWindow::onCollapseAll()
{
    m_treeView->collapseAll();
}
