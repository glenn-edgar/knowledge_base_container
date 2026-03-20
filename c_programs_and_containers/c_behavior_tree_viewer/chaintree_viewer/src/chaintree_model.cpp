#include "chaintree_model.h"
#include <QFont>
#include <QBrush>

ChainTreeModel::ChainTreeModel(QObject *parent)
    : QStandardItemModel(parent)
{
    setHorizontalHeaderLabels({"Node", "Type", "Status", "Path"});
}

QStandardItem *ChainTreeModel::addNode(QStandardItem *parent,
                                        const QString &name,
                                        NodeType type,
                                        NodeStatus status,
                                        const QString &path)
{
    auto *nameItem   = new QStandardItem(name);
    auto *typeItem   = new QStandardItem(nodeTypeName(type));
    auto *statusItem = new QStandardItem(statusName(status));
    auto *pathItem   = new QStandardItem(path);

    // Store metadata in the name item
    nameItem->setData(static_cast<int>(type), NodeTypeRole);
    nameItem->setData(static_cast<int>(status), NodeStatusRole);
    nameItem->setData(path, NodePathRole);

    // Color-code the type column
    typeItem->setForeground(QBrush(nodeTypeColor(type)));

    // Color-code the status column
    statusItem->setForeground(QBrush(statusColor(status)));

    // Bold the name for structural nodes
    if (type == NodeType::Root || type == NodeType::BT_Sequence ||
        type == NodeType::BT_Selector || type == NodeType::BT_Parallel ||
        type == NodeType::SM_Machine || type == NodeType::CF_Chain) {
        QFont f = nameItem->font();
        f.setBold(true);
        nameItem->setFont(f);
    }

    // Make non-name columns read-only
    typeItem->setEditable(false);
    statusItem->setEditable(false);
    pathItem->setEditable(false);
    nameItem->setEditable(false);

    QList<QStandardItem *> row = {nameItem, typeItem, statusItem, pathItem};

    if (parent) {
        parent->appendRow(row);
    } else {
        appendRow(row);
    }

    return nameItem;
}

void ChainTreeModel::loadExampleTree()
{
    clear();
    setHorizontalHeaderLabels({"Node", "Type", "Status", "Path"});

    // Root: Irrigation Controller (the real-world system since 2013)
    auto *root = addNode(nullptr, "Irrigation Controller", NodeType::Root,
                         NodeStatus::Running, "irrigation");

    // --- Behavior Tree: Zone Scheduler ---
    auto *scheduler = addNode(root, "Zone Scheduler", NodeType::BT_Sequence,
                              NodeStatus::Running, "irrigation.scheduler");

    auto *checkWeather = addNode(scheduler, "Check Weather", NodeType::BT_Condition,
                                 NodeStatus::Success, "irrigation.scheduler.check_weather");

    auto *checkMoisture = addNode(scheduler, "Check Soil Moisture", NodeType::BT_Condition,
                                  NodeStatus::Success, "irrigation.scheduler.check_moisture");

    auto *zoneSelector = addNode(scheduler, "Zone Selector", NodeType::BT_Selector,
                                 NodeStatus::Running, "irrigation.scheduler.zone_sel");

    auto *zone1 = addNode(zoneSelector, "Zone 1 - Front Lawn", NodeType::BT_Action,
                          NodeStatus::Running, "irrigation.scheduler.zone_sel.zone1");

    auto *zone2 = addNode(zoneSelector, "Zone 2 - Garden", NodeType::BT_Action,
                          NodeStatus::Idle, "irrigation.scheduler.zone_sel.zone2");

    auto *zone3 = addNode(zoneSelector, "Zone 3 - Back Yard", NodeType::BT_Action,
                          NodeStatus::Idle, "irrigation.scheduler.zone_sel.zone3");

    // --- State Machine: Valve Controller ---
    auto *valveSM = addNode(root, "Valve Controller", NodeType::SM_Machine,
                            NodeStatus::Running, "irrigation.valve_ctrl");

    auto *valveClosed = addNode(valveSM, "Closed", NodeType::SM_State,
                                NodeStatus::Idle, "irrigation.valve_ctrl.closed");

    auto *valveOpening = addNode(valveSM, "Opening", NodeType::SM_State,
                                 NodeStatus::Idle, "irrigation.valve_ctrl.opening");

    auto *valveOpen = addNode(valveSM, "Open", NodeType::SM_State,
                              NodeStatus::Running, "irrigation.valve_ctrl.open");

    auto *valveClosing = addNode(valveSM, "Closing", NodeType::SM_State,
                                 NodeStatus::Idle, "irrigation.valve_ctrl.closing");

    // Transitions
    addNode(valveClosed, "on_start -> Opening", NodeType::SM_Transition,
            NodeStatus::Idle, "irrigation.valve_ctrl.closed.t_start");

    addNode(valveOpening, "on_complete -> Open", NodeType::SM_Transition,
            NodeStatus::Idle, "irrigation.valve_ctrl.opening.t_done");

    addNode(valveOpen, "on_timeout -> Closing", NodeType::SM_Transition,
            NodeStatus::Idle, "irrigation.valve_ctrl.open.t_timeout");

    addNode(valveClosing, "on_complete -> Closed", NodeType::SM_Transition,
            NodeStatus::Idle, "irrigation.valve_ctrl.closing.t_done");

    // --- Sequential Control Flow: Startup Sequence ---
    auto *startup = addNode(root, "Startup Sequence", NodeType::CF_Chain,
                            NodeStatus::Success, "irrigation.startup");

    addNode(startup, "Initialize Hardware", NodeType::CF_Step,
            NodeStatus::Success, "irrigation.startup.init_hw");

    addNode(startup, "Load Configuration", NodeType::CF_Step,
            NodeStatus::Success, "irrigation.startup.load_cfg");

    addNode(startup, "Connect NATS", NodeType::CF_Step,
            NodeStatus::Success, "irrigation.startup.nats_connect");

    addNode(startup, "Publish Online Event", NodeType::CF_Event,
            NodeStatus::Success, "irrigation.startup.evt_online");

    // Suppress unused variable warnings
    (void)checkWeather; (void)checkMoisture;
    (void)zone1; (void)zone2; (void)zone3;
}

QString ChainTreeModel::nodeTypeName(NodeType type)
{
    switch (type) {
    case NodeType::Root:           return "Root";
    case NodeType::BT_Sequence:    return "BT:Sequence";
    case NodeType::BT_Selector:    return "BT:Selector";
    case NodeType::BT_Parallel:    return "BT:Parallel";
    case NodeType::BT_Action:      return "BT:Action";
    case NodeType::BT_Condition:   return "BT:Condition";
    case NodeType::BT_Decorator:   return "BT:Decorator";
    case NodeType::SM_Machine:     return "SM:Machine";
    case NodeType::SM_State:       return "SM:State";
    case NodeType::SM_Transition:  return "SM:Transition";
    case NodeType::CF_Chain:       return "CF:Chain";
    case NodeType::CF_Step:        return "CF:Step";
    case NodeType::CF_Event:       return "CF:Event";
    }
    return "Unknown";
}

QColor ChainTreeModel::nodeTypeColor(NodeType type)
{
    // Brighter, more saturated colors for dark background
    switch (type) {
    case NodeType::Root:           return QColor(210, 210, 210);
    // Behavior tree: bright blue family
    case NodeType::BT_Sequence:    return QColor(100, 160, 255);
    case NodeType::BT_Selector:    return QColor( 80, 180, 255);
    case NodeType::BT_Parallel:    return QColor(120, 140, 240);
    case NodeType::BT_Action:      return QColor(130, 200, 255);
    case NodeType::BT_Condition:   return QColor(160, 220, 255);
    case NodeType::BT_Decorator:   return QColor(110, 170, 255);
    // State machine: bright green family
    case NodeType::SM_Machine:     return QColor( 70, 220, 100);
    case NodeType::SM_State:       return QColor(100, 230, 140);
    case NodeType::SM_Transition:  return QColor(140, 240, 170);
    // Control flow: bright orange family
    case NodeType::CF_Chain:       return QColor(255, 180,  70);
    case NodeType::CF_Step:        return QColor(255, 200, 100);
    case NodeType::CF_Event:       return QColor(255, 220, 130);
    }
    return QColor(180, 180, 180);
}

QString ChainTreeModel::statusName(NodeStatus status)
{
    switch (status) {
    case NodeStatus::Idle:    return "Idle";
    case NodeStatus::Running: return "Running";
    case NodeStatus::Success: return "Success";
    case NodeStatus::Failure: return "Failure";
    }
    return "Unknown";
}

QColor ChainTreeModel::statusColor(NodeStatus status)
{
    switch (status) {
    case NodeStatus::Idle:    return QColor(140, 140, 140);
    case NodeStatus::Running: return QColor( 80, 200, 255);
    case NodeStatus::Success: return QColor( 80, 230, 110);
    case NodeStatus::Failure: return QColor(255,  80,  80);
    }
    return QColor(140, 140, 140);
}