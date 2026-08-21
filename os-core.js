/*
 * ═══════════════════════════════════════════════════════════════
 * TradingOS — Núcleo compartido (os-core.js)
 * ═══════════════════════════════════════════════════════════════
 * Consolida dos piezas que antes estaban duplicadas entre los 5 módulos
 * (Anomaly Radar, Market Pulse, Forex Monitor, El Gusano, Journal):
 *
 *   1. TradingHub — puente de comunicación entre apps (BroadcastChannel + localStorage)
 *      Antes: 19 líneas copiadas literalmente en cada archivo, 1 línea distinta cada vez.
 *      Ahora: un solo lugar. Cada módulo solo llama TradingHub.init(appId, autoSaveMap).
 *
 *   2. OSFetch — fetch con fallback a proxies CORS públicos
 *      Antes: 3 implementaciones distintas con distinta cantidad de proxies (2, 3 y 7).
 *      Ahora: una sola cascada de 7 proxies, disponible para los 3 módulos que la necesitan.
 *
 * USO en cada módulo (reemplaza el bloque TradingHub que tenían antes):
 *
 *   <script src="os-core.js"></script>
 *   <script>
 *     TradingHub.init('gusano', { PROTOCOL_DONE: 'protocol' });
 *     // ... resto del código del módulo, usa TradingHub.emit/.on/.getX() igual que antes
 *   </script>
 *
 * Mapa de autoSave por módulo (para copiar al integrar cada archivo):
 *   anomaly (Anomaly Radar):    { ANOMALY_UPDATE: 'anomaly', ANOMALY_ALERT: 'anomaly' }
 *   pulse   (Market Pulse):     { MACRO_UPDATE: 'macro' }
 *   forex   (Forex Monitor):    { PRICE_UPDATE: 'prices' }
 *   gusano  (El Gusano):        { PROTOCOL_DONE: 'protocol' }
 *   journal (Trading Journal):  { TRADE_OPENED: 'trade', TRADE_CLOSED: 'trade' }
 *
 * Changelog:
 *   v1.0 — extracción inicial desde los 5 módulos existentes. Cero cambios de
 *          comportamiento respecto al código original — misma API pública
 *          (TradingHub.emit/on/save/read/getAnomalyState/etc), mismos nombres
 *          de clave en localStorage, mismo canal de BroadcastChannel.
 * ═══════════════════════════════════════════════════════════════
 */
(function(global){
  'use strict';

  // ── HUB: claves de localStorage compartidas entre todos los módulos ──
  const HUB_KEYS = {
    anomaly:  'hub_anomaly_state',
    macro:    'hub_macro_state',
    protocol: 'hub_protocol_state',
    trade:    'hub_open_trade',
    prices:   'hub_prices'
  };

  let _bc = null;
  try { _bc = new BroadcastChannel('trading-hub'); } catch(e) { /* navegador sin soporte — degradación silenciosa, igual que antes */ }

  let _appId = 'unknown';
  let _autoSaveMap = {}; // ej. {PROTOCOL_DONE: 'protocol'} — reemplaza la línea que antes variaba por archivo

  const hub = {
    /**
     * Debe llamarse una vez al inicio de cada módulo.
     * @param {string} appId - identificador del módulo ('anomaly'|'pulse'|'forex'|'gusano'|'journal')
     * @param {object} autoSaveMap - qué tipos de evento se auto-guardan y bajo qué clave de HUB_KEYS
     */
    init(appId, autoSaveMap){
      _appId = appId || 'unknown';
      _autoSaveMap = autoSaveMap || {};
    },
    save(key, data){
      try{ localStorage.setItem(key, JSON.stringify({...data, _ts:Date.now(), _src:_appId})); }catch(e){}
    },
    read(key){
      try{ const r = localStorage.getItem(key); return r ? JSON.parse(r) : null; }catch(e){ return null; }
    },
    emit(type, payload){
      const msg = {type, payload, src:_appId, ts:Date.now()};
      if(_bc){ try{ _bc.postMessage(msg); }catch(e){} }
      const mappedKey = _autoSaveMap[type];
      if(mappedKey && HUB_KEYS[mappedKey]) hub.save(HUB_KEYS[mappedKey], payload);
    },
    on(type, cb){
      if(!_bc) return;
      _bc.addEventListener('message', e=>{
        if(e.data && e.data.type===type && e.data.src!==_appId) cb(e.data.payload, e.data);
      });
    },
    getAnomalyState(){ return hub.read(HUB_KEYS.anomaly); },
    getMacroState(){ return hub.read(HUB_KEYS.macro); },
    getProtocolState(){ return hub.read(HUB_KEYS.protocol); },
    getOpenTrade(){ return hub.read(HUB_KEYS.trade); },
    getPrices(){ return hub.read(HUB_KEYS.prices); },
  };

  // ── OSFETCH: fetch con fallback a proxies CORS públicos ──
  // v2 — antes esto era una cascada SECUENCIAL simple (probar uno, si falla el siguiente).
  // Al auditar Market Pulse encontré que su propia implementación local era mejor:
  // lanza los proxies EN PARALELO por grupos (Promise.any — devuelve el primero que
  // responda, no espera a que cada uno truene antes de intentar el siguiente). Esa es
  // la que se adopta aquí como base — Anomaly Radar y Forex Monitor pasan a usarla
  // también, en vez de quedarse con sus propias versiones más débiles/lentas.
  //
  // v3 — dos correcciones sobre la v2 original de Market Pulse:
  //   1. Se quitó el header 'User-Agent' falso. Los navegadores no dejan que JS lo
  //      sobreescriba vía fetch(), y aunque lo permitieran no ayudaría: CORS lo decide
  //      el header de RESPUESTA del servidor (Access-Control-Allow-Origin), no qué tan
  //      "real" parezca la petición. Yahoo/FRED simplemente no mandan ese header para
  //      dominios de terceros — ningún header de request lo cambia.
  //   2. "Proxy pegajoso": antes de lanzar el race paralelo completo (3-4 peticiones
  //      simultáneas), se prueba primero, solo, el proxy que funcionó la última vez
  //      (guardado en localStorage, persiste entre sesiones). Si ese sigue vivo, resuelve
  //      en 1 sola petición en vez de 3-7. Si falla, cae al race paralelo completo como
  //      respaldo — no se pierde resiliencia, solo se evita el gasto innecesario cuando
  //      todo está funcionando normal.
  const YF_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'no-cache',
  };
  const STICKY_KEY = 'os_last_good_proxy';

  async function _tryFetch(url, opts={}, timeoutMs=8000){
    const ctrl = new AbortController();
    const t = setTimeout(()=>ctrl.abort(), timeoutMs);
    try{
      const r = await fetch(url, {...opts, signal: ctrl.signal});
      clearTimeout(t);
      if (!r.ok) return null;
      // FIX: r.ok solo confirma el código HTTP (200-299) — no que el CONTENIDO sea
      // válido. Varios proxies CORS devuelven una página de error/límite con status
      // 200 (no un error de red real), que antes se aceptaba como "éxito" y detenía
      // ahí mismo toda la cascada de respaldo — el fallo real solo se descubría
      // después, cuando el llamador intentaba .json() y ya era tarde para probar
      // otro proxy. Ahora se valida el contenido aquí, antes de aceptar la respuesta,
      // así el resto de proxies sí se intentan cuando uno devuelve basura.
      const text = await r.text();
      try{ JSON.parse(text); }
      catch(parseErr){ return null; } // contenido inválido — seguir con el siguiente proxy
      // Reconstruir un Response nuevo con el texto ya validado, para que el llamador
      // pueda seguir usando .json() normalmente — mismo contrato de siempre.
      return new Response(text, { status: r.status, headers: r.headers });
    }catch(e){
      clearTimeout(t);
      return null;
    }
  }

  // Proxies nombrados — el nombre es lo que se guarda como "pegajoso" en localStorage.
  const NAMED_PROXIES = [
    { name:'corsproxy',   group:'A', fn: url => _tryFetch('https://corsproxy.io/?' + encodeURIComponent(url), {}, 10000) },
    { name:'allorigins',  group:'A', fn: url => _tryFetch('https://api.allorigins.win/raw?url=' + encodeURIComponent(url), {}, 10000) },
    { name:'corssh',      group:'A', fn: url => _tryFetch('https://proxy.cors.sh/' + url, { headers: { 'x-requested-with': 'XMLHttpRequest' } }, 10000) },
    { name:'codetabs',    group:'B', fn: url => _tryFetch('https://api.codetabs.com/v1/proxy?quest=' + encodeURIComponent(url), {}, 10000) },
    { name:'thingproxy',  group:'B', fn: url => _tryFetch('https://thingproxy.freeboard.io/fetch/' + url, {}, 10000) },
    { name:'allorigins2', group:'B', fn: async url => {
        // allorigins /get — devuelve el JSON envuelto en {contents:"..."}, hay que desenvolverlo
        const r = await _tryFetch('https://api.allorigins.win/get?url=' + encodeURIComponent(url), {}, 10000);
        if (!r) return null;
        try{
          const j = await r.json();
          if (!j.contents) return null;
          return new Response(j.contents, { status: 200, headers: { 'Content-Type': 'application/json' } });
        }catch(e){ return null; }
      } },
    { name:'proxyscrape', group:'B', fn: url => _tryFetch('https://api.proxyscrape.com/v2/?url=' + encodeURIComponent(url), {}, 10000) },
  ];

  function getStickyProxy(){ try{ return localStorage.getItem(STICKY_KEY); }catch(e){ return null; } }
  function setStickyProxy(name){ try{ localStorage.setItem(STICKY_KEY, name); }catch(e){} }

  async function _raceGroup(url, group){
    const attempts = NAMED_PROXIES.filter(p => p.group === group);
    const wrapped = attempts.map(p => p.fn(url).then(r => r ? {r, name:p.name} : Promise.reject()));
    return await Promise.any(wrapped).catch(() => null);
  }

  /**
   * Fetch con fallback robusto: directo → proxy pegajoso (el que funcionó la última
   * vez) → grupo A en paralelo → grupo B en paralelo. Devuelve un objeto Response —
   * el caller hace su propio .json(). Lanza si todo falla.
   * @param {string} url - URL objetivo (sin proxy)
   * @returns {Promise<Response>}
   */
  async function fetchWithProxy(url){
    // Proxy pegajoso primero — si ya sabemos cuál funcionó la última vez, probarlo
    // solo, antes de gastar el intento directo (que ya sabemos que casi siempre falla
    // para Yahoo/FRED) o el race paralelo completo.
    const sticky = getStickyProxy();
    if (sticky){
      const p = NAMED_PROXIES.find(x => x.name === sticky);
      if (p){
        const r = await p.fn(url);
        if (r) return r; // sigue vivo — resuelto en 1 sola petición
      }
    }

    // Intento directo — casi siempre falla para Yahoo/FRED por CORS (esperado, no es
    // error). Se deja con timeout corto porque sabemos que rara vez sirve para estos
    // dos, pero no cuesta caro intentarlo por si algún día cambian su política, o si
    // OSFetch se usa para llamar a algún otro endpoint que sí soporte CORS directo.
    const direct = await _tryFetch(url, { headers: YF_HEADERS }, 4000);
    if (direct) return direct;

    // Grupo A en paralelo
    const resultA = await _raceGroup(url, 'A');
    if (resultA) { setStickyProxy(resultA.name); return resultA.r; }

    // Grupo B en paralelo (respaldo si A falló por completo)
    const resultB = await _raceGroup(url, 'B');
    if (resultB) { setStickyProxy(resultB.name); return resultB.r; }

    throw new Error('Sin conexión — todos los proxies fallaron para: '+url);
  }

  /** Conveniencia: igual que fetchWithProxy pero ya parseado a JSON. */
  async function fetchWithProxyJson(url){
    const r = await fetchWithProxy(url);
    return r.json();
  }

  global.TradingHub = hub;
  global.OSFetch = { fetchWithProxy, fetchWithProxyJson };

  // ── NAV: menú flotante para ir de cualquier app a cualquier otra ──
  // Todas viven en la misma carpeta del repo (mismo dominio de GitHub Pages),
  // así que los links son simples nombres de archivo relativos — no rutas
  // completas, para que funcione igual en local y en producción.
  const APPS = [
    { id:'hub',     name:'Hub',             icon:'⬡', file:'index.html' },
    { id:'anomaly', name:'Anomaly Radar',   icon:'📡', file:'anomaly_radar_v5-9.html' },
    { id:'pulse',   name:'Market Pulse',    icon:'📊', file:'trading-dashboard-v21.html' },
    { id:'forex',   name:'Forex Monitor',   icon:'💱', file:'forex_tracker-v4.html' },
    { id:'gusano',  name:'El Gusano',       icon:'🐛', file:'el-gusano-protocolo-4-v5.html' },
    { id:'journal', name:'Trading Journal', icon:'📒', file:'trading_journal_v3-9-0.html' },
    { id:'matf',    name:'MA Timeframe Calc', icon:'φ', file:'calculadora_ma_tf.html' },
  ];

  function injectNav(currentAppId){
    if (document.getElementById('tos-nav-root')) return; // evita duplicados si se llama 2 veces

    // Esquina inferior IZQUIERDA a propósito — la derecha ya la usan los toasts
    // de Anomaly Radar (#toast-container) y Trading Journal (.toast), ambos en
    // bottom:20px/right:20px. z-index:400, por debajo de los modales existentes
    // (ej. .modal-overlay de Trading Journal usa 500) para que un modal abierto
    // siempre tape el botón, nunca al revés.
    const root = document.createElement('div');
    root.id = 'tos-nav-root';
    root.style.cssText = 'position:fixed;bottom:16px;left:16px;z-index:400;font-family:system-ui,sans-serif';

    const btn = document.createElement('button');
    btn.textContent = '⬡ Apps';
    btn.style.cssText = 'background:#12121c;color:#e8e8f0;border:1px solid #3a3a52;border-radius:20px;padding:8px 16px;font-size:12px;font-weight:600;cursor:pointer;box-shadow:0 4px 16px rgba(0,0,0,0.4)';

    const menu = document.createElement('div');
    menu.style.cssText = 'display:none;position:absolute;bottom:44px;left:0;background:#12121c;border:1px solid #3a3a52;border-radius:10px;padding:6px;min-width:180px;box-shadow:0 8px 24px rgba(0,0,0,0.5)';

    APPS.forEach(app => {
      const isCurrent = app.id === currentAppId;
      const item = document.createElement(isCurrent ? 'div' : 'a');
      if (!isCurrent) item.href = app.file;
      item.textContent = `${app.icon} ${app.name}`;
      item.style.cssText = `display:block;padding:8px 12px;border-radius:6px;font-size:12px;text-decoration:none;
        color:${isCurrent ? '#00ff96' : '#c8c8dc'};
        background:${isCurrent ? 'rgba(0,255,150,0.08)' : 'transparent'};
        font-weight:${isCurrent ? '700' : '400'};cursor:${isCurrent ? 'default' : 'pointer'}`;
      if (!isCurrent) {
        item.onmouseenter = () => item.style.background = 'rgba(255,255,255,0.06)';
        item.onmouseleave = () => item.style.background = 'transparent';
      }
      menu.appendChild(item);
    });

    btn.onclick = (e) => {
      e.stopPropagation();
      menu.style.display = menu.style.display === 'none' ? 'block' : 'none';
    };
    document.addEventListener('click', () => { menu.style.display = 'none'; });

    root.appendChild(menu);
    root.appendChild(btn);
    document.body.appendChild(root);
  }

  global.TradingHub.injectNav = injectNav;

})(window);
