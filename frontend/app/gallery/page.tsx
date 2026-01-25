export default function GalleryPage() {
  return (
    <main className="page">
      <section className="gallery">
        <div className="section-head">
          <div>
            <p className="eyebrow">Реалізації</p>
            <h2>Від шоу-руму до треку</h2>
          </div>
          <div className="dots">
            <span className="dot active"></span>
            <span className="dot"></span>
            <span className="dot"></span>
          </div>
        </div>
        <div className="gallery-grid">
          <div className="shot wide"></div>
          <div className="shot tall"></div>
          <div className="shot"></div>
          <div className="shot"></div>
          <div className="shot wide"></div>
        </div>
      </section>
    </main>
  );
}
