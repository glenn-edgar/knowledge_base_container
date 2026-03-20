#ifndef MAINWINDOW_H
#define MAINWINDOW_H

#include <QMainWindow>
#include <QTreeView>
#include <QLabel>
#include "chaintree_model.h"

class MainWindow : public QMainWindow {
    Q_OBJECT

public:
    explicit MainWindow(QWidget *parent = nullptr);

private slots:
    void onNodeClicked(const QModelIndex &index);
    void onExpandAll();
    void onCollapseAll();

private:
    QTreeView      *m_treeView;
    ChainTreeModel *m_model;
    QLabel         *m_statusLabel;  // shows ltree path of selected node
};

#endif // MAINWINDOW_H
