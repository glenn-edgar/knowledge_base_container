#include <QApplication>
#include <QPalette>
#include <QStyleFactory>
#include "mainwindow.h"

static void applyDarkTheme(QApplication &app)
{
    app.setStyle(QStyleFactory::create("Fusion"));

    QPalette dark;
    QColor darkBg(30, 30, 30);
    QColor midBg(45, 45, 45);
    QColor lightText(220, 220, 220);
    QColor dimText(160, 160, 160);
    QColor accent(80, 140, 220);
    QColor highlight(60, 120, 200);

    dark.setColor(QPalette::Window,          darkBg);
    dark.setColor(QPalette::WindowText,      lightText);
    dark.setColor(QPalette::Base,            QColor(25, 25, 25));
    dark.setColor(QPalette::AlternateBase,   QColor(38, 38, 38));
    dark.setColor(QPalette::ToolTipBase,     midBg);
    dark.setColor(QPalette::ToolTipText,     lightText);
    dark.setColor(QPalette::Text,            lightText);
    dark.setColor(QPalette::Button,          midBg);
    dark.setColor(QPalette::ButtonText,      lightText);
    dark.setColor(QPalette::BrightText,      Qt::white);
    dark.setColor(QPalette::Link,            accent);
    dark.setColor(QPalette::Highlight,       highlight);
    dark.setColor(QPalette::HighlightedText, Qt::white);

    // Disabled state
    dark.setColor(QPalette::Disabled, QPalette::Text,       dimText);
    dark.setColor(QPalette::Disabled, QPalette::ButtonText,  dimText);
    dark.setColor(QPalette::Disabled, QPalette::WindowText,  dimText);

    app.setPalette(dark);

    // Fine-tune with stylesheet
    app.setStyleSheet(
        "QToolTip { color: #dcdcdc; background-color: #2d2d2d; "
        "           border: 1px solid #555; }"
        "QTreeView { border: none; }"
        "QTreeView::item:hover { background: #3a3a3a; }"
        "QTreeView::item:selected { background: #3c78c8; }"
        "QHeaderView::section { background: #2d2d2d; color: #dcdcdc; "
        "                       border: 1px solid #444; padding: 4px; }"
        "QStatusBar { border-top: 1px solid #444; }"
        "QToolBar { border-bottom: 1px solid #444; spacing: 6px; }"
    );
}

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    app.setApplicationName("ChainTree Viewer");
    app.setOrganizationName("Onyx Engineering");

    applyDarkTheme(app);

    MainWindow window;
    window.show();

    return app.exec();
}