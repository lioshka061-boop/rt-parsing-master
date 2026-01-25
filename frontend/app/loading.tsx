import './globals.css';

export default function Loading() {
  return (
    <div className="route-loader" role="status" aria-live="polite">
      <span className="route-loader__bar" />
      <span className="route-loader__label">Завантаження…</span>
    </div>
  );
}
