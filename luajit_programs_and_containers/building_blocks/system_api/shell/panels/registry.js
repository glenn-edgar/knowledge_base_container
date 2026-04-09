// registry.js — Panel registration, show/hide, popup lifecycle.
//
// Manages fullscreen panels (one active at a time) and popup overlays.
// Panels are lazy-imported ES modules with init(container, ctx) / destroy().
// Each panel can carry a domain override (site, siteBucket) so the same
// panel module can be instantiated for different domains.

export class PanelManager {
  constructor(ctx) {
    this._ctx = ctx;
    this._panels = {};
    this._activeFullscreen = null;
    this._activePopups = {};
    this._fullscreenEl = document.getElementById("panel-fullscreen");
    this._popupLayer = document.getElementById("panel-popup-layer");
    this._legacyFrame = document.getElementById("legacy-frame");
    this._onDomainChange = null;  // callback when active domain changes
    this._activeDomain = null;
  }

  // Register a panel. type: "fullscreen" | "popup"
  // opts.domain: { name, site, ... } — domain context override
  register(id, opts) {
    this._panels[id] = {
      id: id,
      type: opts.type || "fullscreen",
      modulePath: opts.modulePath,
      module: null,
      domain: opts.domain || null,
    };
  }

  // Build per-panel ctx, merging domain-specific site/siteBucket.
  _ctxForPanel(panel) {
    if (!panel.domain || !panel.domain.site) return this._ctx;
    var site = panel.domain.site;
    return Object.assign({}, this._ctx, {
      site: site,
      siteBucket: site.replace(/\./g, "_"),
      domain: panel.domain,
    });
  }

  // Show a fullscreen panel (hides current, destroys old).
  async show(id) {
    var panel = this._panels[id];
    if (!panel) { console.error("Unknown panel:", id); return; }

    if (panel.type === "popup") {
      return this.showPopup(id);
    }

    // Close any open popups
    this.hideAllPopups();

    // Hide legacy iframe
    if (this._legacyFrame) {
      this._legacyFrame.style.display = "none";
      this._legacyFrame.src = "about:blank";
    }

    // Destroy current fullscreen panel
    if (this._activeFullscreen && this._activeFullscreen !== id) {
      this._destroyPanel(this._activeFullscreen);
    }

    // Lazy-import module
    if (!panel.module) {
      var mod = await import(panel.modulePath);
      panel.module = mod.default;
    }

    // Create container and init
    this._fullscreenEl.innerHTML = "";
    this._fullscreenEl.style.display = "block";
    var container = document.createElement("div");
    container.className = "panel-content";
    this._fullscreenEl.appendChild(container);

    this._activeFullscreen = id;
    var ctx = this._ctxForPanel(panel);
    await panel.module.init(container, ctx);

    // Notify domain change
    if (panel.domain && panel.domain.name !== this._activeDomain) {
      this._activeDomain = panel.domain.name;
      if (this._onDomainChange) this._onDomainChange(panel.domain);
    }
  }

  // Show a popup overlay (does not hide fullscreen panel).
  async showPopup(id) {
    var panel = this._panels[id];
    if (!panel) { console.error("Unknown panel:", id); return; }

    // Already open?
    if (this._activePopups[id]) return;

    // Lazy-import module
    if (!panel.module) {
      var mod = await import(panel.modulePath);
      panel.module = mod.default;
    }

    // Build popup DOM
    var backdrop = document.createElement("div");
    backdrop.className = "popup-backdrop";

    var popupEl = document.createElement("div");
    popupEl.className = "popup-panel";

    var header = document.createElement("div");
    header.className = "popup-header";
    header.innerHTML =
      '<span class="popup-title">' + (panel.module.label || id) + '</span>' +
      '<button class="popup-close">\u2715</button>';
    popupEl.appendChild(header);

    var body = document.createElement("div");
    body.className = "popup-body";
    popupEl.appendChild(body);

    this._popupLayer.appendChild(backdrop);
    this._popupLayer.appendChild(popupEl);
    this._popupLayer.style.display = "block";

    // Dismiss handlers
    var self = this;
    backdrop.addEventListener("click", function () { self.hidePopup(id); });
    header.querySelector(".popup-close").addEventListener("click", function () { self.hidePopup(id); });

    this._activePopups[id] = { backdrop: backdrop, popupEl: popupEl };

    var ctx = this._ctxForPanel(panel);
    await panel.module.init(body, ctx);
  }

  // Hide and destroy a popup.
  hidePopup(id) {
    var popup = this._activePopups[id];
    if (!popup) return;

    var panel = this._panels[id];
    if (panel && panel.module && panel.module.destroy) {
      panel.module.destroy();
    }

    popup.backdrop.remove();
    popup.popupEl.remove();
    delete this._activePopups[id];

    if (Object.keys(this._activePopups).length === 0) {
      this._popupLayer.style.display = "none";
    }
  }

  // Hide all popups.
  hideAllPopups() {
    for (var id in this._activePopups) {
      this.hidePopup(id);
    }
  }

  // Show legacy iframe (for unmigrated tabs).
  showLegacy(src) {
    // Close any open popups
    this.hideAllPopups();

    // Destroy current fullscreen panel
    if (this._activeFullscreen) {
      this._destroyPanel(this._activeFullscreen);
      this._activeFullscreen = null;
    }

    this._fullscreenEl.style.display = "none";
    this._legacyFrame.style.display = "block";
    this._legacyFrame.src = src;
  }

  // Internal: destroy a panel.
  _destroyPanel(id) {
    var panel = this._panels[id];
    if (panel && panel.module && panel.module.destroy) {
      panel.module.destroy();
    }
    this._fullscreenEl.innerHTML = "";
  }
}
