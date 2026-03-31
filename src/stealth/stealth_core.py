from __future__ import annotations

import hashlib
import json
from typing import Any

STEALTH_SCRIPT = r"""
(() => {
  const seed = __STEALTH_SEED__;
  const deleteFromPrototypeChain = (root, key) => {
    let current = root;
    while (current) {
      try {
        if (Object.prototype.hasOwnProperty.call(current, key)) {
          delete current[key];
        }
      } catch (_error) {}
      current = Object.getPrototypeOf(current);
    }
  };
  const defineGetter = (target, key, value) => {
    if (!target) {
      return;
    }
    try {
      Object.defineProperty(target, key, {
        get: () => value,
        configurable: true,
      });
    } catch (_error) {}
  };
  const makeListenerHost = () => ({
    addListener: () => undefined,
    removeListener: () => undefined,
    hasListener: () => false,
    hasListeners: () => false,
  });
  const makeArrayLike = (items, tag, nameKey) => {
    const array = items.slice();
    Object.defineProperty(array, "item", {
      value: (index) => array[index] || null,
      configurable: true,
    });
    Object.defineProperty(array, "namedItem", {
      value: (name) => array.find((item) => item && item[nameKey] === name) || null,
      configurable: true,
    });
    Object.defineProperty(array, "refresh", {
      value: () => undefined,
      configurable: true,
    });
    try {
      Object.defineProperty(array, Symbol.toStringTag, {
        value: tag,
        configurable: true,
      });
    } catch (_error) {}
    return array;
  };

  const navigatorProto = Object.getPrototypeOf(navigator);
  deleteFromPrototypeChain(navigator, "webdriver");
  defineGetter(navigator, "webdriver", undefined);
  defineGetter(navigatorProto, "webdriver", undefined);

  const pdfPlugin = {
    name: "Chrome PDF Viewer",
    filename: "internal-pdf-viewer",
    description: "Portable Document Format",
    __mimeTypes: [],
  };
  const naclPlugin = {
    name: "Native Client",
    filename: "internal-nacl-plugin",
    description: "",
    __mimeTypes: [],
  };

  const mimePdf = {
    type: "application/pdf",
    suffixes: "pdf",
    description: "Portable Document Format",
    enabledPlugin: pdfPlugin,
  };
  const mimeChromePdf = {
    type: "application/x-google-chrome-pdf",
    suffixes: "pdf",
    description: "Portable Document Format",
    enabledPlugin: pdfPlugin,
  };
  const mimeNacl = {
    type: "application/x-nacl",
    suffixes: "",
    description: "Native Client Executable",
    enabledPlugin: naclPlugin,
  };
  const mimePnacl = {
    type: "application/x-pnacl",
    suffixes: "",
    description: "Portable Native Client Executable",
    enabledPlugin: naclPlugin,
  };

  pdfPlugin.__mimeTypes.push(mimePdf, mimeChromePdf);
  naclPlugin.__mimeTypes.push(mimeNacl, mimePnacl);

  const mimeTypes = makeArrayLike(
    [mimePdf, mimeChromePdf, mimeNacl, mimePnacl].map((mime) => {
      try {
        Object.defineProperty(mime, Symbol.toStringTag, {
          value: "MimeType",
          configurable: true,
        });
      } catch (_error) {}
      return mime;
    }),
    "MimeTypeArray",
    "type"
  );

  const plugins = makeArrayLike(
    [pdfPlugin, naclPlugin].map((plugin) => {
      plugin.__mimeTypes.forEach((mime, index) => {
        plugin[index] = mime;
      });
      Object.defineProperty(plugin, "length", {
        value: plugin.__mimeTypes.length,
        configurable: true,
      });
      Object.defineProperty(plugin, "item", {
        value: (index) => plugin[index] || null,
        configurable: true,
      });
      Object.defineProperty(plugin, "namedItem", {
        value: (name) =>
          plugin.__mimeTypes.find((mime) => mime.type === name) || null,
        configurable: true,
      });
      try {
        Object.defineProperty(plugin, Symbol.toStringTag, {
          value: "Plugin",
          configurable: true,
        });
      } catch (_error) {}
      delete plugin.__mimeTypes;
      return plugin;
    }),
    "PluginArray",
    "name"
  );

  defineGetter(navigator, "plugins", plugins);
  defineGetter(navigatorProto, "plugins", plugins);
  defineGetter(navigator, "mimeTypes", mimeTypes);
  defineGetter(navigatorProto, "mimeTypes", mimeTypes);

  window.chrome = window.chrome || {};
  if (!window.chrome.runtime) {
    window.chrome.runtime = {
      OnInstalledReason: {
        CHROME_UPDATE: "chrome_update",
        INSTALL: "install",
        SHARED_MODULE_UPDATE: "shared_module_update",
        UPDATE: "update",
      },
      OnRestartRequiredReason: {
        APP_UPDATE: "app_update",
        OS_UPDATE: "os_update",
        PERIODIC: "periodic",
      },
      PlatformArch: {
        ARM: "arm",
        ARM64: "arm64",
        MIPS: "mips",
        MIPS64: "mips64",
        X86_32: "x86-32",
        X86_64: "x86-64",
      },
      PlatformNaclArch: {
        ARM: "arm",
        MIPS: "mips",
        MIPS64: "mips64",
        X86_32: "x86-32",
        X86_64: "x86-64",
      },
      PlatformOs: {
        ANDROID: "android",
        CROS: "cros",
        LINUX: "linux",
        MAC: "mac",
        OPENBSD: "openbsd",
        WIN: "win",
      },
      RequestUpdateCheckStatus: {
        NO_UPDATE: "no_update",
        THROTTLED: "throttled",
        UPDATE_AVAILABLE: "update_available",
      },
      connect: () => ({
        name: "",
        onDisconnect: makeListenerHost(),
        onMessage: makeListenerHost(),
        postMessage: () => undefined,
        disconnect: () => undefined,
      }),
      sendMessage: (...args) => {
        const callback = args.find((arg) => typeof arg === "function");
        if (callback) {
          callback();
        }
      },
      getManifest: () => ({
        manifest_version: 3,
        name: "Google Chrome",
        version: "123.0.0.0",
      }),
      getURL: (path = "") =>
        `chrome-extension://invalid/${String(path).replace(/^\/+/, "")}`,
      id: undefined,
      onConnect: makeListenerHost(),
      onInstalled: makeListenerHost(),
      onMessage: makeListenerHost(),
      onStartup: makeListenerHost(),
    };
  }

  if (navigator.permissions && typeof navigator.permissions.query === "function") {
    try {
      const originalQuery = navigator.permissions.query.bind(navigator.permissions);
      navigator.permissions.query = (parameters) => {
        if (parameters && parameters.name === "notifications") {
          const notificationState =
            typeof Notification === "undefined"
              ? "prompt"
              : Notification.permission === "granted"
                ? "granted"
                : Notification.permission === "denied"
                  ? "denied"
                  : "prompt";
          return Promise.resolve({
            onchange: null,
            state: notificationState,
          });
        }
        return originalQuery(parameters);
      };
    } catch (_error) {}
  }

  try {
    delete window._playwright;
  } catch (_error) {}
  try {
    delete window._pw_manual;
  } catch (_error) {}

  const webglProfiles = [
    {
      vendor: "Intel Inc.",
      renderer: "Intel Iris OpenGL Engine",
    },
    {
      vendor: "Google Inc. (NVIDIA)",
      renderer: "ANGLE (NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0)",
    },
    {
      vendor: "Google Inc. (AMD)",
      renderer: "ANGLE (AMD Radeon RX 580 Series Direct3D11 vs_5_0 ps_5_0)",
    },
    {
      vendor: "Google Inc. (Intel)",
      renderer: "ANGLE (Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
    },
  ];
  const selectedProfile = webglProfiles[Math.abs(Number(seed) || 0) % webglProfiles.length];
  const patchWebGL = (proto) => {
    if (!proto || typeof proto.getParameter !== "function") {
      return;
    }
    const originalGetParameter = proto.getParameter;
    Object.defineProperty(proto, "getParameter", {
      value: function(parameter) {
        if (parameter === 37445) {
          return selectedProfile.vendor;
        }
        if (parameter === 37446) {
          return selectedProfile.renderer;
        }
        return originalGetParameter.apply(this, arguments);
      },
      configurable: true,
    });
  };

  patchWebGL(window.WebGLRenderingContext && window.WebGLRenderingContext.prototype);
  patchWebGL(window.WebGL2RenderingContext && window.WebGL2RenderingContext.prototype);
})();
"""


def _webgl_seed(username: str) -> int:
    normalized = str(username or "default").strip().lower() or "default"
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _script_with_seed(username: str) -> str:
    return STEALTH_SCRIPT.replace("__STEALTH_SEED__", json.dumps(_webgl_seed(username)))


def patch_context(context: Any, username: str):
    return context.add_init_script(script=_script_with_seed(username))


def patch_page(page: Any, username: str):
    return page.add_init_script(script=_script_with_seed(username))
