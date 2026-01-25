export default function HowWeWorkPage() {
  return (
    <main className="page">
      <section className="story">
        <div className="text">
          <p className="eyebrow">Як ми працюємо</p>
          <h2>Від сканування до монтажу</h2>
          <p>
            Будуємо каталог на основі точних даних: парсимо постачальників, нормалізуємо назви, додаємо фото
            та категорії, прописуємо доставку і наявність.
          </p>
          <ul>
            <li>3D-сканування кузова → точні лекала</li>
            <li>Фільтрація товарів: у наявності / під замовлення</li>
            <li>Опис, переклади, знижки та множники цін</li>
            <li>Доставка 14–60 днів, консультації по сумісності</li>
          </ul>
          <div className="hero-actions">
            <a className="primary" href="/login">
              Увійти в адмінку
            </a>
            <a className="ghost" href="/register">
              Спробувати безкоштовно
            </a>
          </div>
        </div>
        <div className="media">
          <div className="panel large"></div>
          <div className="panel small"></div>
        </div>
      </section>
    </main>
  );
}
