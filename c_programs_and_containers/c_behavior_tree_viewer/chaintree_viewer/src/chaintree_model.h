#ifndef CHAINTREE_MODEL_H
#define CHAINTREE_MODEL_H

#include <QStandardItemModel>
#include <QColor>

// Node types matching ChainTree architecture
enum class NodeType {
    Root,
    // Behavior tree nodes
    BT_Sequence,
    BT_Selector,
    BT_Parallel,
    BT_Action,
    BT_Condition,
    BT_Decorator,
    // State machine nodes
    SM_Machine,
    SM_State,
    SM_Transition,
    // Sequential control flow
    CF_Chain,
    CF_Step,
    CF_Event,
};

// Custom roles for storing node metadata
enum ChainTreeRoles {
    NodeTypeRole = Qt::UserRole + 1,
    NodeStatusRole,      // running, success, failure, idle
    NodePathRole,        // ltree-style path (e.g. "root.irrigation.zone1")
};

// Possible runtime status values
enum class NodeStatus {
    Idle,
    Running,
    Success,
    Failure,
};

class ChainTreeModel : public QStandardItemModel {
    Q_OBJECT

public:
    explicit ChainTreeModel(QObject *parent = nullptr);

    // Convenience: add a typed node under a parent
    QStandardItem *addNode(QStandardItem *parent,
                           const QString &name,
                           NodeType type,
                           NodeStatus status = NodeStatus::Idle,
                           const QString &path = QString());

    // Populate with example ChainTree structure
    void loadExampleTree();

    // Helpers
    static QString nodeTypeName(NodeType type);
    static QColor  nodeTypeColor(NodeType type);
    static QString statusName(NodeStatus status);
    static QColor  statusColor(NodeStatus status);
};

#endif // CHAINTREE_MODEL_H
