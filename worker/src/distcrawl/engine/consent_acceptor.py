"""heuristic cookie consent acceptor."""

COOKIE_ACCEPTOR_JS = r"""(function () {
  'use strict';

  const TAG = '[cookie-consent-acceptor]';
  console.info(TAG, 'script injected and starting...');

  // most-specific first
  const ACCEPT_PHRASES = [
    // english
    'accept all cookies and continue',
    'accept all cookies',
    'accept all & close',
    'accept all and close',
    'accept and continue',
    'agree to all cookies',
    'allow all cookies',
    'i agree to all',
    'accept all',
    'allow all',
    'i accept all',
    'accept cookies',
    'agree to cookies',
    'allow cookies',
    'i accept cookies',
    'i agree',
    'i accept',
    'accept & close',
    'accept and close',
    'accept',
    'agree',
    'allow',
    'consent',
    'got it',
    'ok',
    // german
    'alle cookies akzeptieren',
    'alle cookies zulassen',
    'alle cookies erlauben',
    'alle cookies annehmen',
    'alles akzeptieren',
    'alles zulassen',
    'alle akzeptieren',
    'alle zustimmen',
    'allen zustimmen',
    'cookies akzeptieren',
    'cookies zulassen',
    'zustimmen und weiter',
    'ich stimme zu',
    'zustimmen',
    'akzeptieren',
    'erlauben',
    'einverstanden',
    // french
    'accepter tous les cookies',
    'accepter tous',
    'tout accepter',
    'tout autoriser',
    'autoriser tout',
    "j'accepte tout",
    "j'accepte",
    "je suis d'accord",
    'accepter les cookies',
    'accepter',
    'autoriser',
    "d'accord",
    // spanish
    'aceptar todas las cookies',
    'aceptar todo',
    'aceptar todas',
    'permitir todo',
    'acepto todo',
    'acepto las cookies',
    'acepto',
    'aceptar',
    'permitir',
    'de acuerdo',
    // italian
    'accetta tutti i cookie',
    'accetta tutto',
    'accetta tutti',
    'accetto tutto',
    'accetta',
    // dutch
    'alle cookies accepteren',
    'alle cookies toestaan',
    'alles accepteren',
    'alles toestaan',
    'accepteer alles',
    'cookies accepteren',
    'accepteren',
    'accepteer',
    'toestaan',
    // polish
    'zaakceptuj wszystkie cookies',
    'akceptuję wszystkie',
    'zaakceptuj wszystkie',
    'akceptuję',
    'zgadzam się',
    // russian
    'принять все файлы cookie',
    'принять все куки',
    'разрешить все',
    'принять все',
    'согласен со всем',
    'принять',
    'разрешить',
    'согласен',
  ];

  const SELECTOR = 'button, [role="button"], a, input[type="submit"], input[type="button"]';

  // falls back to aria-label or title
  function getElementText(el) {
    let raw;
    if (el.tagName === 'INPUT') {
      raw = el.value || el.getAttribute('aria-label') || '';
    } else {
      raw = el.innerText || el.textContent || '';
      if (!raw.trim()) {
        raw = el.getAttribute('aria-label') || el.getAttribute('title') || '';
      }
    }
    return raw.trim().toLowerCase().replace(/\s+/g, ' ');
  }

  // space-padding prevents partial-word hits
  function matches(text, phrase) {
    if (text === phrase) return true;
    return (' ' + text + ' ').includes(' ' + phrase + ' ');
  }

  function isVisible(el) {
    if (el.getAttribute('aria-hidden') === 'true') return false;
    if (el.offsetParent === null && el.offsetWidth === 0 && el.offsetHeight === 0) {
      return false;
    }
    const r = el.getBoundingClientRect();
    return r.width > 0 || r.height > 0;
  }

  function tryAccept() {
    // rate limit: max one click per second to prevent infinite loops
    // on badly programmed sites where the observer fires constantly.
    const now = Date.now();
    if (now - lastClickTime < 1000) return false;

    const candidates = Array.from(document.querySelectorAll(SELECTOR));
    for (const phrase of ACCEPT_PHRASES) {
      for (const el of candidates) {
        if (!isVisible(el)) continue;
        if (el.hasAttribute('disabled') || el.getAttribute('aria-disabled') === 'true') continue;
        if (el.tagName === 'A') {
          if (el.getAttribute('target') === '_blank') continue;
          var href = (el.getAttribute('href') || '').trim();
          if (href && href !== '#' && !href.startsWith('javascript:')) continue;
        }
        const text = getElementText(el);
        if (!text) continue;
        if (matches(text, phrase)) {
          console.info(
            TAG,
            'clicking <' + el.tagName.toLowerCase() + '> "' + text + '"',
            '(matched phrase: "' + phrase + '")',
            el.outerHTML.substring(0, 100)
          );
          lastClickTime = Date.now();
          try {
            if (typeof el.click === 'function') {
              el.click();
            } else {
              el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
            }
          } catch (e) {
            console.warn(TAG, 'click failed:', e.message);
          }
          if (typeof window.handleCookieAccept === 'function') {
            try {
              window.handleCookieAccept(
                window.location.href,
                Date.now()
              );
            } catch (e) {
              console.warn(TAG, 'handleCookieAccept failed:', e.message);
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  let accepted = false;
  let debounceTimer = null;
  let observer = null;
  let lastClickTime = 0;

  function scheduleAccept() {
    if (accepted) return;
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(function () {
      if (accepted) return;
      if (tryAccept()) {
        accepted = true;
        if (observer) {
          observer.disconnect();
          observer = null;
        }
        console.info(TAG, 'observer disconnected after successful accept');
      }
    }, 150);
  }

  function setup() {
    if (tryAccept()) { // try right away
      accepted = true;
      console.info(TAG, 'accepted synchronously on setup');
      return;
    }

    observer = new MutationObserver(scheduleAccept); // watch for banners added after load
    observer.observe(document.documentElement, {
      childList: true,
      subtree: true,
    });
    console.info(TAG, 'MutationObserver installed');

    setTimeout(scheduleAccept, 500);  // retry for slow-loading banners
    setTimeout(scheduleAccept, 2000);

    setTimeout(function () { // stop after 30s
      if (observer && !accepted) {
        observer.disconnect();
        observer = null;
        console.info(TAG, 'observer stopped after 30 s timeout');
      }
    }, 30000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', setup);
  } else {
    setup();
  }
})();
"""
