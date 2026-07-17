(function () {
  'use strict';

  const pause = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

  async function showPage(name) {
    if (typeof window.switchPage === 'function') window.switchPage(name);
    await pause(360);
  }

  async function showUtility(name) {
    await showPage('utils');
    if (typeof window.utilitiesShowView === 'function') window.utilitiesShowView(name);
    if (name === 'resale' && typeof window.utilitiesShowStage === 'function') {
      window.utilitiesShowStage('search');
    }
    if (name === 'deals' && typeof window.dealShowStage === 'function') {
      window.dealShowStage('search');
    }
    await pause(180);
  }

  async function showConfigTab(name) {
    const button = document.querySelector(`.cfg-tab[data-cfg="${name}"]`);
    if (button && typeof window.cfgTab === 'function') window.cfgTab(button);
    await pause(160);
  }

  window.LZT_TUTORIALS = {
    version: 1,
    groups: [
      { id: 'main', title: 'Основные страницы' },
      { id: 'settings', title: 'Настройки' },
      { id: 'utilities', title: 'Утилиты' },
    ],
    tours: [
      {
        id: 'overview',
        group: 'main',
        accent: 'general',
        title: 'Общий интерфейс',
        description: 'Навигация, общие менеджеры и оформление.',
        steps: [
          { selector: '.tb-pages', title: 'Разделы программы', text: 'Здесь переключаются поднятия, проверки и отдельные утилиты.' },
          { selector: '.tb-managers', title: 'Общие менеджеры', text: 'Аккаунты, прокси и монитор API доступны с любой страницы.' },
          { selector: '#theme-toggle', title: 'Оформление', text: 'Переключает тёмную, светлую и LZT-тему.' },
        ],
      },
      {
        id: 'bump',
        group: 'main',
        accent: 'bump',
        title: 'Поднятие',
        description: 'Лимиты, расписание и задачи поднятия.',
        prepare: async () => {
          await showPage('bump');
          if (typeof window.closeTaskDetail === 'function') window.closeTaskDetail();
          await pause(120);
        },
        steps: [
          { selector: '.smart', title: 'Умный лимит', text: 'Показывает расход поднятий каждого аккаунта и управляет авто-распределением.' },
          { selector: '.smart-limit-edit', title: 'Лимиты аккаунтов', text: 'Открой менеджер и укажи отдельный суточный лимит для каждого аккаунта.' },
          { selector: '.dist-schedule', title: 'Расписание поднятий', text: 'Выбери режим распределения. При включённой автоматике расписание подстроится под доступный лимит.' },
          { selector: '.monitor-head', title: 'Монитор задач', text: 'Здесь создаются задачи и сразу видно их общее количество.' },
          { selector: '.monitor-toolbar', title: 'Поиск и сортировка', text: 'Фильтруй задачи по состоянию, владельцу и ближайшему запуску.' },
          { selector: '#task-grid', title: 'Карточки задач', text: 'Карточка показывает владельца, таймер и быстрые действия. Нажми её для подробной истории.' },
        ],
      },
      {
        id: 'checks',
        group: 'main',
        accent: 'checks',
        title: 'Проверка',
        description: 'Опрос покупок, этапы проверки и результаты.',
        prepare: async () => {
          await showPage('arb');
          if (typeof window.arbTab === 'function') window.arbTab('checks');
          await pause(150);
        },
        steps: [
          { selector: '#arb-status', title: 'Состояние сервиса', text: 'Одним нажатием включает или останавливает автоматические проверки.' },
          { selector: '.arb-toolbar', title: 'Управление', text: 'Здесь находятся общий поиск, ручные операции, настройки и очистка данных.' },
          { selector: '#arb-purchase', title: 'Опрос покупок', text: 'Показывает время следующего опроса. Кнопкой справа его можно запустить сразу.' },
          { selector: '.arb-stats', title: 'Краткая статистика', text: 'Карточки показывают очередь, ошибки, невалид и КТ. На них можно нажимать.' },
          { selector: '.arb-tabs', title: 'Результаты работы', text: 'Проверки, пролив, гарантии, арбитражи, невалидные аккаунты и технические ошибки разделены по вкладкам.' },
          { selector: '#apane-checks .a-filters', title: 'Фильтр проверок', text: 'Выбери нужный процент гарантии или найди конкретный аккаунт по ID.' },
          { selector: '#apane-checks .atable-wrap', title: 'Расписание проверок', text: 'Здесь видны время запуска, остаток и действия для каждой проверки.' },
        ],
      },
      {
        id: 'bump-settings',
        group: 'settings',
        accent: 'bump',
        title: 'Аккаунты поднятий',
        description: 'Токен, личный лимит, цвет и прокси.',
        prepare: async () => {
          await showPage('bump');
          if (typeof window.openTokenModal === 'function') await window.openTokenModal(false, null);
          if (typeof window.setTokenFormOpen === 'function') window.setTokenFormOpen(true);
          await pause(180);
        },
        cleanup: () => {
          if (typeof window.closeTokenModal === 'function') window.closeTokenModal();
        },
        steps: [
          { selector: '#token-modal .mhead', title: 'Менеджер аккаунтов', text: 'Здесь хранятся аккаунты LZT, которые используются задачами поднятия и утилитами.' },
          { selector: '#tok-name', title: 'Понятное название', text: 'Укажи никнейм или короткое имя — оно будет видно на карточках задач и в списках.' },
          { selector: '#add-tok-form .secret-inline', title: 'API-токен LZT', text: 'Токен сохраняется локально в зашифрованном виде. Кнопка справа временно показывает его.' },
          { selector: '#tok-daily-limit', title: 'Личный лимит', text: 'Это отдельный суточный запас поднятий именно этого аккаунта. Умный лимит учитывает его автоматически.' },
          { selector: '#color-swatches', title: 'Цвет аккаунта', text: 'Цвет помогает быстро отличать владельцев задач в мониторе поднятий.' },
          { selector: '#csel-tok-proxy', title: 'Прокси аккаунта', text: 'Необязательный прокси можно выбрать по названию или IP. Новые задачи получат его автоматически.' },
        ],
      },
      {
        id: 'task-settings',
        group: 'settings',
        accent: 'bump',
        title: 'Создание задачи',
        description: 'Тип поднятия, количество и интервал.',
        prepare: async () => {
          await showPage('bump');
          if (typeof window.openTaskModal === 'function') window.openTaskModal();
          await pause(180);
        },
        cleanup: () => {
          if (typeof window.closeTaskModal === 'function') window.closeTaskModal();
        },
        steps: [
          { selector: '#tm-name', title: 'Название задачи', text: 'Задай короткое понятное название. Оно не изменится при выборе другого аккаунта.' },
          { selector: '#csel-tok', title: 'Аккаунт LZT', text: 'Выбери сохранённый аккаунт. Ручной токен нужен только для разовой задачи.' },
          { selector: '#csel-tm-proxy', title: 'Прокси задачи', text: 'Можно оставить прокси аккаунта, выбрать другой или работать без него.' },
          { selector: '.target-mode-tabs', title: 'Тип задачи', text: 'Ссылка с фильтром поднимает найденные объявления, а точечный режим работает только с указанными ID.' },
          { selector: '.task-run-grid', title: 'Пачка и интервал', text: 'Укажи, сколько разных аккаунтов поднимать за запуск и через сколько минут повторять задачу.' },
          { selector: '.task-modal-box .mfoot', title: 'Сохранение', text: 'После создания задача сразу появится в мониторе. Перед сохранением проверь аккаунт, режим и интервал.' },
        ],
      },
      {
        id: 'check-settings',
        group: 'settings',
        accent: 'checks',
        title: 'Настройки проверки',
        description: 'Опрос, этапы, пролив, уведомления и очистка.',
        contextSelector: '#arb-config-modal .cfg-tab.active',
        contextPrefix: 'Открыта вкладка',
        prepare: async () => {
          await showPage('arb');
          if (typeof window.openArbConfig === 'function') await window.openArbConfig('main');
          const modal = document.getElementById('arb-config-modal');
          if (modal && getComputedStyle(modal).display === 'none' && typeof window.openM === 'function') window.openM('arb-config-modal');
          await showConfigTab('main');
        },
        cleanup: () => {
          if (typeof window.closeM === 'function') window.closeM('arb-config-modal');
        },
        steps: [
          { selector: '#arb-config-modal .cfg-tabs', title: 'Разделы настроек', text: 'Настройки разделены по назначению. Инструкция сама переключит вкладки и покажет главное.' },
          { selector: '#cfg-sec-main .secret-card', title: 'Основные · токен', text: 'Этот токен используется сервисом проверки. Он хранится локально и защищён Windows DPAPI.' },
          { selector: '#cf-link', title: 'Основные · покупки', text: 'Вставь ссылку на свои покупки. Интервал опроса и остановка на страницах без гарантий находятся ниже.' },
          { selector: '#cf-steam-key', title: 'Steam', text: 'Steam Web API Key нужен для проверки профиля, ограничений и КТ.' , prepare: () => showConfigTab('steam') },
          { selector: '#cf-percent-list', title: 'Проверки', text: 'Проценты задают моменты проверки внутри гарантии: например, 10%, 55% и 99% её длительности.', prepare: () => showConfigTab('checks') },
          { selector: '#cfg-sec-proliv > .cfg-row:first-child', title: 'Пролив · включение', text: 'Включи автоматическое перевыставление. После окончания гарантии аккаунт перейдёт в очередь пролива и будет опубликован с параметрами ниже.', prepare: () => showConfigTab('proliv') },
          { selector: '#cf-pl-timing', title: 'Пролив · время и попытки', text: 'Задержка задаётся в секундах после окончания гарантии. «Максимум попыток» ограничивает повторные запуски при временных ошибках; после последней попытки аккаунт появится во вкладке «Ошибки».', prepare: () => showConfigTab('proliv') },
          { selector: '#cf-pl-tag-row', title: 'Пролив · метка', text: 'Необязательная метка добавляется только после успешной публикации. Выключи переключатель, если метка этому объявлению не нужна.', prepare: () => showConfigTab('proliv') },
          { selector: '#cf-pl-publish-fields', title: 'Пролив · объявление', text: 'Здесь задаются заголовок и цена нового объявления. Категория ниже определяет раздел Маркета; подсказка «!» показывает доступные ID категорий.', prepare: () => showConfigTab('proliv') },
          { selector: '#cfg-sec-proliv .extra-games-panel', title: 'Пролив · скрытые игры', text: 'Отметь только те дополнительные игры, которые нужно добавить при публикации. Кнопки «Все» и «Снять» быстро меняют весь список.', prepare: () => showConfigTab('proliv') },
          { selector: '#cf-tg-bots', title: 'Telegram', text: 'Добавь ботов, выбери отдельный канал ошибок и настрой шаблоны уведомлений.', prepare: () => showConfigTab('telegram') },
          { selector: '#cf-tr-users', title: 'Передача', text: 'Менеджер пользователей определяет, кому можно передавать аккаунты вручную или автоматически.', prepare: () => showConfigTab('transfer') },
          { selector: '#cfg-sec-cleanup', title: 'Очистка', text: 'Удаляй только выбранные рабочие данные или полностью очищай сервис с обязательным подтверждением.', prepare: () => showConfigTab('cleanup') },
        ],
      },
      {
        id: 'utilities',
        group: 'utilities',
        accent: 'utilities',
        title: 'Страница утилит',
        description: 'Выбор отдельного инструмента.',
        prepare: async () => {
          await showPage('utils');
          if (typeof window.utilitiesShowView === 'function') window.utilitiesShowView('hub');
          await pause(120);
        },
        steps: [
          { selector: '.utils-hero', title: 'Локальные инструменты', text: 'Утилиты работают внутри LZT Control и не отправляют данные сторонним сервисам.' },
          { selector: '.utilities-grid', title: 'Выбор утилиты', text: 'В каждой карточке кратко указано назначение инструмента. Нажми «Открыть утилиту», чтобы начать.' },
        ],
      },
      {
        id: 'resale',
        group: 'utilities',
        accent: 'utilities',
        title: 'Аудит покупок',
        description: 'Поиск перепродаж и финансовая статистика.',
        prepare: () => showUtility('resale'),
        steps: [
          { selector: '#utility-resale .utility-about', title: 'Назначение', text: 'Утилита связывает покупку с последующей перепродажей и помогает найти забытые аккаунты.' },
          { selector: '#uf-orders-url', title: 'Ссылка на покупки', text: 'Вставь ссылку на историю покупок или выбери ранее сохранённую ссылку.' },
          { selector: '#utility-resale .utility-source-switch', title: 'API-токен', text: 'Можно выбрать аккаунт из общего менеджера или временно вставить другой токен.' },
          { selector: '#utility-resale .frow', title: 'Диапазон страниц', text: 'Укажи первую и последнюю страницу покупок, которую нужно обработать.' },
          { selector: '#utility-resale .utility-form-actions', title: 'Запуск поиска', text: 'Поиск можно остановить. Уже полученные страницы останутся в текущей сессии.' },
          { selector: '#utility-resale .utility-panel:not(.utility-form)', title: 'Прогресс и журнал', text: 'Справа отображаются страницы, число запросов и понятный ход операции.' },
        ],
      },
      {
        id: 'steam-kt',
        group: 'utilities',
        accent: 'utilities',
        title: 'Проверка Steam на КТ',
        description: 'Массовая проверка Steam-профилей.',
        prepare: () => showUtility('steam-kt'),
        steps: [
          { selector: '#ukt-input', title: 'Исходные данные', text: 'Вставляй строки в любом формате. Главное — наличие ссылки Steam-профиля.' },
          { selector: '.steam-kt-actions', title: 'Запуск проверки', text: 'Запускает пакетную проверку. Поле можно очистить отдельно от результатов.' },
          { selector: '.steam-kt-console', title: 'Журнал проверки', text: 'Показывает пачки и ответы Steam API по мере выполнения.' },
          { selector: '.steam-kt-results-panel .steam-kt-stats', title: 'Итог', text: 'Сразу видно количество чистых профилей, КТ и ошибок.' },
          { selector: '.steam-kt-results-panel .steam-kt-tools', title: 'Фильтры результатов', text: 'Фильтруй по виду ограничения, продавцу и формату нужных ссылок.' },
          { selector: '.steam-kt-result-actions', title: 'Действия', text: 'Можно получить продавцов, проставить метку видимым КТ, скопировать или скачать результат.' },
        ],
      },
      {
        id: 'deals',
        group: 'utilities',
        accent: 'utilities',
        title: 'Поиск выгодных предложений',
        description: 'Сравнение продажи и цены скупщика.',
        prepare: () => showUtility('deals'),
        steps: [
          { selector: '.deal-guide', title: 'Перед запуском', text: 'Сначала обнови цены скупщиков для нужных аккаунтов на LZT Market.' },
          { selector: '.deal-mode', title: 'Режим анализа', text: 'Реселлер учитывает покупку и перепродажу. Продавец сравнивает текущую цену со скупщиком.' },
          { selector: '#utility-deals .utility-source-switch', title: 'API-токен', text: 'Выбери сохранённый аккаунт или используй токен только для этого запуска.' },
          { selector: '#ud-url', title: 'Ссылка на продажи', text: 'Вставь свою ссылку или открой менеджер сохранённых ссылок.' },
          { selector: '.deal-pages', title: 'Страницы продаж', text: 'Ограничь диапазон, если не нужно проверять весь список.' },
          { selector: '.deal-actions', title: 'Запуск анализа', text: 'После запуска утилита соберёт цены, даты и рассчитает обе стратегии.' },
          { selector: '.deal-process-card', title: 'Ход операции', text: 'Прогресс и журнал показывают текущую пачку и результат запросов.' },
        ],
      },
      {
        id: 'mass-claims',
        group: 'utilities',
        accent: 'utilities',
        title: 'Массовые арбитражи',
        description: 'Подготовка и запуск очереди претензий.',
        prepare: () => showUtility('mass-claims'),
        steps: [
          { selector: '.mass-claims-guide', title: 'Безопасная подготовка', text: 'Сначала формируется предварительная очередь. Запросы на создание пойдут только после подтверждения.' },
          { selector: '#utility-mass-claims .utility-source-switch', title: 'API-токен', text: 'Используй аккаунт из менеджера или временный токен.' },
          { selector: '.mass-claims-mode', title: 'Целевой ID', text: 'Арбитраж можно создать на исходный ID или на предыдущего продавца из цепочки Same ID.' },
          { selector: '#uc-description', title: 'Описание', text: 'Этот текст будет отправлен в каждый арбитраж очереди.' },
          { selector: '#uc-links', title: 'Аккаунты', text: 'Вставь ссылки или ID. Повторы и лишний текст будут убраны автоматически.' },
          { selector: '.mass-claims-interval', title: 'Интервал', text: 'Пауза между арбитражами защищает от превышения лимитов API.' },
          { selector: '.mass-claims-actions', title: 'Подготовить очередь', text: 'Проверь найденные ID справа и только затем подтверждай создание.' },
          { selector: '.mass-claims-layout > .mass-claims-panel:last-child', title: 'Очередь и журнал', text: 'Здесь видны готовые, созданные и проблемные записи, а также ход операции.' },
        ],
      },
    ],
  };
}());
