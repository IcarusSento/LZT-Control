(function () {
  'use strict';

  const config = window.LZT_TUTORIALS;
  if (!config || !Array.isArray(config.tours)) return;

  const state = {
    menuOpen: false,
    safetyOpen: false,
    safetyReturnToMenu: false,
    running: false,
    tour: null,
    index: 0,
    target: null,
    contextTarget: null,
    completed: readCompleted(),
    raf: 0,
  };

  let launcher;
  let menuLayer;
  let menuBody;
  let safetyLayer;
  let stage;
  let focus;
  let contextFocus;
  let card;
  let contextLabel;
  let cardTitle;
  let cardText;
  let stepCount;
  let progress;
  let previousButton;
  let nextButton;

  function readCompleted() {
    try {
      const value = JSON.parse(localStorage.getItem(`lzt-tutorial-completed-v${config.version}`) || '[]');
      return new Set(Array.isArray(value) ? value : []);
    } catch (_) {
      return new Set();
    }
  }

  function saveCompleted() {
    try {
      localStorage.setItem(`lzt-tutorial-completed-v${config.version}`, JSON.stringify([...state.completed]));
    } catch (_) {}
  }

  function safetyWasSeen() {
    try {
      return localStorage.getItem(`lzt-tutorial-safety-v${config.version}`) === '1';
    } catch (_) {
      return false;
    }
  }

  function rememberSafety() {
    try {
      localStorage.setItem(`lzt-tutorial-safety-v${config.version}`, '1');
    } catch (_) {}
  }

  function icon(name) {
    if (name === 'close') return '<svg viewBox="0 0 24 24"><path d="m7 7 10 10M17 7 7 17"/></svg>';
    if (name === 'shield') return '<svg viewBox="0 0 24 24"><path d="M12 3 4.5 6v5.2c0 4.7 3 8.1 7.5 9.8 4.5-1.7 7.5-5.1 7.5-9.8V6L12 3Z"/><path d="m8.7 12 2.1 2.1 4.7-5"/></svg>';
    return '<svg viewBox="0 0 24 24"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v16H6.5A2.5 2.5 0 0 0 4 21.5v-16Z"/><path d="M4 5.5v16M8 7h8M8 11h6"/></svg>';
  }

  function build() {
    const managers = document.querySelector('.tb-managers');
    if (!managers) return;

    launcher = document.createElement('button');
    launcher.className = 'tb-manager tutorial-launcher';
    launcher.type = 'button';
    launcher.setAttribute('aria-label', 'Открыть интерактивную инструкцию');
    launcher.dataset.tooltip = 'Интерактивная инструкция';
    launcher.innerHTML = `${icon('book')}<span class="tb-manager-label">Инструкция</span>`;
    launcher.addEventListener('click', openEntry);
    managers.appendChild(launcher);

    menuLayer = document.createElement('div');
    menuLayer.className = 'tutorial-menu-layer';
    menuLayer.setAttribute('aria-hidden', 'true');
    menuLayer.innerHTML = `
      <section class="tutorial-menu" role="dialog" aria-modal="true" aria-labelledby="tutorial-menu-title">
        <header class="tutorial-menu-head">
          <div class="tutorial-menu-icon">${icon('book')}</div>
          <div class="tutorial-menu-copy"><h2 id="tutorial-menu-title">Интерактивная инструкция</h2><p>Выбери раздел. Подсказки запускаются только вручную и ничего не меняют в настройках.</p></div>
          <button class="tutorial-close" type="button" aria-label="Закрыть">${icon('close')}</button>
        </header>
        <div class="tutorial-menu-body"></div>
      </section>`;
    document.body.appendChild(menuLayer);
    menuBody = menuLayer.querySelector('.tutorial-menu-body');
    menuLayer.querySelector('.tutorial-close').addEventListener('click', closeMenu);
    installBackdropClose(menuLayer, closeMenu);

    safetyLayer = document.createElement('div');
    safetyLayer.className = 'tutorial-safety-layer';
    safetyLayer.setAttribute('aria-hidden', 'true');
    safetyLayer.innerHTML = `
      <section class="tutorial-safety" role="dialog" aria-modal="true" aria-labelledby="tutorial-safety-title">
        <header class="tutorial-safety-head">
          <div class="tutorial-safety-icon">${icon('shield')}</div>
          <div><span>Перед началом</span><h2 id="tutorial-safety-title">Безопасность ваших данных</h2></div>
          <button class="tutorial-close tutorial-safety-x" type="button" aria-label="Закрыть">${icon('close')}</button>
        </header>
        <div class="tutorial-safety-body">
          <div class="tutorial-safety-lead">
            <h3>Доверяйте не только программе, но и месту, где она запущена</h3>
            <p>За последние месяцы некоторые пользователи Маркета столкнулись с кражей средств с баланса, аккаунтов и других данных. Частым фактором в подобных случаях становились удалённые серверы, сборки и доступы, полученные от неизвестных людей.</p>
          </div>
          <div class="tutorial-safety-grid">
            <article><i>01</i><div><b>Выбирайте надёжную среду</b><p>Используйте личный компьютер или сервер крупного известного дата-центра. Не доверяйте серверам из Telegram, от случайных продавцов, неизвестных работодателей и посредников.</p></div></article>
            <article><i>02</i><div><b>Как LZT Control хранит секреты</b><p>API-токены, пароли прокси и другие секреты сохраняются локально и шифруются Windows DPAPI — они привязаны к текущему пользователю Windows.</p></div></article>
            <article><i>03</i><div><b>Не запускайте неизвестный код</b><p>Не устанавливайте сторонние программы, скрипты и расширения с непроверенных сайтов, из соцсетей, YouTube, TikTok или от неизвестных пользователей.</p></div></article>
          </div>
          <div class="tutorial-safety-note"><strong>Важно</strong><span>DPAPI защищает секреты на диске, но не может спасти данные, если вредоносная программа уже запущена от вашего имени или посторонний человек управляет вашей Windows-сессией.</span></div>
          <p class="tutorial-safety-farewell">Берегите себя, свои аккаунты и данные.</p>
        </div>
        <footer class="tutorial-safety-actions"><button class="tutorial-safety-later" type="button">Закрыть</button><button class="tutorial-safety-accept" type="button">Понятно, перейти к инструкции</button></footer>
      </section>`;
    document.body.appendChild(safetyLayer);
    safetyLayer.querySelector('.tutorial-safety-x').addEventListener('click', () => closeSafety(false));
    safetyLayer.querySelector('.tutorial-safety-later').addEventListener('click', () => closeSafety(false));
    safetyLayer.querySelector('.tutorial-safety-accept').addEventListener('click', () => {
      rememberSafety();
      closeSafety(true);
    });
    installBackdropClose(safetyLayer, () => closeSafety(false));

    stage = document.createElement('div');
    stage.className = 'tutorial-stage';
    stage.setAttribute('aria-hidden', 'true');
    stage.innerHTML = `
      <div class="tutorial-shield"></div>
      <div class="tutorial-focus" aria-hidden="true"></div>
      <div class="tutorial-context-focus" aria-hidden="true"></div>
      <section class="tutorial-card" role="dialog" aria-modal="true" aria-live="polite">
        <div class="tutorial-card-top"><span class="tutorial-step-count"></span><button class="tutorial-stop" type="button">Завершить</button></div>
        <div class="tutorial-context-label" hidden></div>
        <h3></h3><p></p>
        <div class="tutorial-progress"><i></i></div>
        <div class="tutorial-actions"><button class="tutorial-btn previous" type="button">Назад</button><button class="tutorial-btn next" type="button">Далее</button></div>
      </section>`;
    document.body.appendChild(stage);
    focus = stage.querySelector('.tutorial-focus');
    contextFocus = stage.querySelector('.tutorial-context-focus');
    card = stage.querySelector('.tutorial-card');
    contextLabel = card.querySelector('.tutorial-context-label');
    cardTitle = card.querySelector('h3');
    cardText = card.querySelector('p');
    stepCount = card.querySelector('.tutorial-step-count');
    progress = card.querySelector('.tutorial-progress i');
    previousButton = card.querySelector('.previous');
    nextButton = card.querySelector('.next');
    previousButton.addEventListener('click', previous);
    nextButton.addEventListener('click', next);
    card.querySelector('.tutorial-stop').addEventListener('click', finish);

    document.addEventListener('keydown', handleKeydown, true);
    window.addEventListener('resize', schedulePosition, { passive: true });
    document.addEventListener('scroll', schedulePosition, { passive: true, capture: true });
  }

  function currentTourId() {
    const isVisible = (selector) => {
      const element = document.querySelector(selector);
      return !!element && element.getClientRects().length > 0;
    };
    if (isVisible('#page-bump')) return 'bump';
    if (isVisible('#page-arb')) return 'checks';
    if (isVisible('#utility-resale')) return 'resale';
    if (isVisible('#utility-steam-kt')) return 'steam-kt';
    if (isVisible('#utility-deals')) return 'deals';
    if (isVisible('#utility-mass-claims')) return 'mass-claims';
    if (isVisible('#page-utils')) return 'utilities';
    return '';
  }

  function renderMenu() {
    if (!menuBody) return;
    const current = currentTourId();
    const groups = config.groups.map((group) => {
      const tours = config.tours.filter((tour) => tour.group === group.id);
      if (!tours.length) return '';
      return `<section class="tutorial-group"><h3 class="tutorial-group-title">${escapeHtml(group.title)}</h3><div class="tutorial-list">${tours.map((tour) => {
        const done = state.completed.has(tour.id);
        const accent = tour.accent || 'general';
        return `<button class="tutorial-choice${tour.id === current ? ' current' : ''}${done ? ' done' : ''}" type="button" data-tour-id="${escapeHtml(tour.id)}" data-accent="${escapeHtml(accent)}"><b>${escapeHtml(tour.title)}</b><span>${escapeHtml(tour.description || '')}</span><i>${done ? '✓' : '›'}</i></button>`;
      }).join('')}</div></section>`;
    }).join('') || '<div class="tutorial-empty">Инструкции пока не добавлены.</div>';
    menuBody.innerHTML = `${groups}<div class="tutorial-menu-safety"><div class="tutorial-menu-safety-icon">${icon('shield')}</div><div><b>Безопасность данных</b><span>Повторно открыть рекомендации по безопасному запуску.</span></div><button class="tutorial-reopen-safety" type="button">Открыть</button></div>`;
    menuBody.querySelectorAll('[data-tour-id]').forEach((button) => {
      button.addEventListener('click', () => start(button.dataset.tourId));
    });
    menuBody.querySelector('.tutorial-reopen-safety')?.addEventListener('click', () => openSafety(true));
  }

  function openEntry() {
    if (safetyWasSeen()) openMenu();
    else openSafety(false);
  }

  function openMenu() {
    if (state.running) finish();
    if (typeof window.hideSiteTooltip === 'function') window.hideSiteTooltip();
    renderMenu();
    state.menuOpen = true;
    menuLayer.classList.add('open');
    menuLayer.setAttribute('aria-hidden', 'false');
    launcher.classList.add('is-active');
    window.setTimeout(() => menuLayer.querySelector('.tutorial-close')?.focus(), 20);
  }

  function closeMenu() {
    if (!state.menuOpen) return;
    state.menuOpen = false;
    menuLayer.classList.remove('open');
    menuLayer.setAttribute('aria-hidden', 'true');
    launcher.classList.remove('is-active');
    launcher.focus();
  }

  function openSafety(returnToMenu) {
    if (state.running) finish();
    if (typeof window.hideSiteTooltip === 'function') window.hideSiteTooltip();
    state.safetyReturnToMenu = !!returnToMenu;
    state.safetyOpen = true;
    state.menuOpen = false;
    menuLayer.classList.remove('open');
    menuLayer.setAttribute('aria-hidden', 'true');
    safetyLayer.classList.add('open');
    safetyLayer.setAttribute('aria-hidden', 'false');
    launcher.classList.add('is-active');
    window.setTimeout(() => safetyLayer.querySelector('.tutorial-safety-accept')?.focus(), 20);
  }

  function closeSafety(continueToMenu) {
    if (!state.safetyOpen) return;
    const showMenu = !!continueToMenu || state.safetyReturnToMenu;
    state.safetyOpen = false;
    state.safetyReturnToMenu = false;
    safetyLayer.classList.remove('open');
    safetyLayer.setAttribute('aria-hidden', 'true');
    if (showMenu) openMenu();
    else {
      launcher.classList.remove('is-active');
      launcher.focus();
    }
  }

  function installBackdropClose(layer, close) {
    let startedOutside = false;
    layer.addEventListener('pointerdown', (event) => {
      startedOutside = event.target === layer;
    });
    layer.addEventListener('pointerup', (event) => {
      const cleanOutsideClick = startedOutside && event.target === layer;
      startedOutside = false;
      if (cleanOutsideClick) close();
    });
    layer.addEventListener('pointercancel', () => {
      startedOutside = false;
    });
  }

  async function start(id) {
    const tour = config.tours.find((item) => item.id === id);
    if (!tour || !tour.steps?.length) return;
    state.menuOpen = false;
    menuLayer.classList.remove('open');
    menuLayer.setAttribute('aria-hidden', 'true');
    state.tour = tour;
    state.index = 0;
    state.running = true;
    stage.dataset.accent = tour.accent || 'general';
    launcher.classList.add('is-active');
    document.documentElement.classList.add('tutorial-running');
    if (typeof window.hideSiteTooltip === 'function') window.hideSiteTooltip();
    try {
      if (typeof tour.prepare === 'function') await tour.prepare();
    } catch (_) {}
    stage.classList.add('open');
    stage.setAttribute('aria-hidden', 'false');
    await showStep(0, 1);
  }

  async function showStep(index, direction) {
    if (!state.running || !state.tour) return;
    if (index < 0) index = 0;
    if (index >= state.tour.steps.length) {
      complete();
      return;
    }
    const step = state.tour.steps[index];
    card.classList.remove('ready');
    try {
      if (typeof step.prepare === 'function') await step.prepare();
    } catch (_) {}
    const target = await findTarget(step.selector);
    if (!state.running) return;
    if (!target) {
      await showStep(index + (direction < 0 ? -1 : 1), direction);
      return;
    }
    state.index = index;
    state.target = target;
    const contextSelector = step.contextSelector || state.tour.contextSelector;
    const contextTarget = contextSelector ? document.querySelector(contextSelector) : null;
    state.contextTarget = contextTarget && isVisible(contextTarget) ? contextTarget : null;
    const contextName = String(step.contextLabel || state.contextTarget?.textContent || '').trim();
    if (contextName) {
      const prefix = step.contextPrefix || state.tour.contextPrefix || 'Раздел';
      contextLabel.textContent = `${prefix}: ${contextName}`;
      contextLabel.hidden = false;
      contextFocus.textContent = contextName;
    } else {
      contextLabel.hidden = true;
      contextFocus.classList.remove('visible');
      contextFocus.textContent = '';
    }
    try {
      target.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
    } catch (_) {
      target.scrollIntoView();
    }
    await wait(280);
    if (!state.running) return;
    cardTitle.textContent = step.title || state.tour.title;
    cardText.textContent = step.text || '';
    stepCount.textContent = `${state.tour.title} · ${index + 1} / ${state.tour.steps.length}`;
    progress.style.width = `${((index + 1) / state.tour.steps.length) * 100}%`;
    previousButton.disabled = index === 0;
    nextButton.textContent = index === state.tour.steps.length - 1 ? 'Готово' : 'Далее';
    position();
    requestAnimationFrame(() => card.classList.add('ready'));
    nextButton.focus({ preventScroll: true });
  }

  async function findTarget(selector) {
    for (let attempt = 0; attempt < 18; attempt += 1) {
      const element = document.querySelector(selector);
      if (element && isVisible(element)) return element;
      await wait(70);
    }
    return null;
  }

  function isVisible(element) {
    const style = getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity || 1) !== 0 && rect.width > 1 && rect.height > 1;
  }

  function next() {
    if (!state.running) return;
    if (state.index >= state.tour.steps.length - 1) complete();
    else showStep(state.index + 1, 1);
  }

  function previous() {
    if (!state.running || state.index <= 0) return;
    showStep(state.index - 1, -1);
  }

  function complete() {
    if (state.tour) {
      state.completed.add(state.tour.id);
      saveCompleted();
    }
    finish();
  }

  function finish() {
    const finishedTour = state.tour;
    state.running = false;
    state.tour = null;
    state.target = null;
    state.contextTarget = null;
    stage?.classList.remove('open');
    stage?.setAttribute('aria-hidden', 'true');
    if (stage) delete stage.dataset.accent;
    card?.classList.remove('ready');
    contextFocus?.classList.remove('visible');
    if (contextLabel) contextLabel.hidden = true;
    launcher?.classList.remove('is-active');
    document.documentElement.classList.remove('tutorial-running');
    try {
      if (typeof finishedTour?.cleanup === 'function') finishedTour.cleanup();
    } catch (_) {}
    launcher?.focus({ preventScroll: true });
  }

  function handleKeydown(event) {
    if (state.running) {
      if (event.key === 'Escape') {
        event.preventDefault();event.stopImmediatePropagation();finish();
      } else if (event.key === 'ArrowRight' || event.key === 'Enter') {
        event.preventDefault();event.stopImmediatePropagation();next();
      } else if (event.key === 'ArrowLeft') {
        event.preventDefault();event.stopImmediatePropagation();previous();
      }
      return;
    }
    if (state.menuOpen && event.key === 'Escape') {
      event.preventDefault();event.stopImmediatePropagation();closeMenu();
    } else if (state.safetyOpen && event.key === 'Escape') {
      event.preventDefault();event.stopImmediatePropagation();closeSafety(false);
    }
  }

  function schedulePosition() {
    if (!state.running || !state.target) return;
    cancelAnimationFrame(state.raf);
    state.raf = requestAnimationFrame(position);
  }

  function position() {
    if (!state.running || !state.target || !isVisible(state.target)) return;
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const margin = 10;
    const gap = 14;
    const rect = state.target.getBoundingClientRect();
    const left = Math.max(5, rect.left - 7);
    const top = Math.max(5, rect.top - 7);
    const right = Math.min(viewportWidth - 5, rect.right + 7);
    const bottom = Math.min(viewportHeight - 5, rect.bottom + 7);
    focus.style.left = `${Math.round(left)}px`;
    focus.style.top = `${Math.round(top)}px`;
    focus.style.width = `${Math.max(2, Math.round(right - left))}px`;
    focus.style.height = `${Math.max(2, Math.round(bottom - top))}px`;
    // The source sections use different radii (some large layout blocks have 0),
    // but the guide frame should always belong to one visual system.
    focus.style.borderRadius = '14px';

    if (state.contextTarget && isVisible(state.contextTarget)) {
      const contextRect = state.contextTarget.getBoundingClientRect();
      if (contextRect.bottom > 0 && contextRect.top < viewportHeight) {
        contextFocus.style.left = `${Math.round(contextRect.left)}px`;
        contextFocus.style.top = `${Math.round(contextRect.top)}px`;
        contextFocus.style.width = `${Math.round(contextRect.width)}px`;
        contextFocus.style.height = `${Math.round(contextRect.height)}px`;
        contextFocus.classList.add('visible');
      } else contextFocus.classList.remove('visible');
    } else contextFocus.classList.remove('visible');

    const cardRect = card.getBoundingClientRect();
    const cardWidth = cardRect.width || Math.min(350, viewportWidth - 24);
    const cardHeight = cardRect.height || 190;
    let cardLeft = rect.left + (rect.width - cardWidth) / 2;
    let cardTop;
    if (bottom + gap + cardHeight <= viewportHeight - margin) cardTop = bottom + gap;
    else if (top - gap - cardHeight >= margin) cardTop = top - gap - cardHeight;
    else if (right + gap + cardWidth <= viewportWidth - margin) {
      cardLeft = right + gap;cardTop = rect.top + (rect.height - cardHeight) / 2;
    } else if (left - gap - cardWidth >= margin) {
      cardLeft = left - gap - cardWidth;cardTop = rect.top + (rect.height - cardHeight) / 2;
    } else cardTop = Math.max(margin, viewportHeight - cardHeight - margin);
    cardLeft = Math.max(margin, Math.min(cardLeft, viewportWidth - cardWidth - margin));
    cardTop = Math.max(margin, Math.min(cardTop, viewportHeight - cardHeight - margin));
    card.style.left = `${Math.round(cardLeft)}px`;
    card.style.top = `${Math.round(cardTop)}px`;
  }

  function wait(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>'"]/g, (character) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
    })[character]);
  }

  window.LZTTutorial = { open: openEntry, start, stop: finish, safety: () => openSafety(true) };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build, { once: true });
  else build();
}());
