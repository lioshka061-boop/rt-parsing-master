'use client';

type Step = {
  title: string;
  subtitle: string;
  icon: string;
};

const steps: Step[] = [
  { title: 'Кошик', subtitle: 'Підтвердження товарів', icon: 'ri-shopping-cart-2-line' },
  { title: 'Контактні дані', subtitle: 'Отримувач і доставка', icon: 'ri-user-3-line' },
  { title: 'Оплата', subtitle: 'Вибір способу оплати', icon: 'ri-bank-card-line' },
  { title: 'Перевірка', subtitle: 'Перевірте замовлення', icon: 'ri-clipboard-line' },
  { title: 'Успіх', subtitle: 'Замовлення прийняте', icon: 'ri-check-line' },
];

export function CheckoutSteps({ current }: { current: number }) {
  return (
    <ol className="checkout-steps">
      {steps.map((step, index) => {
        const position = index + 1;
        const state =
          current === position ? 'active' : current > position ? 'done' : 'pending';
        return (
          <li key={step.title} className={`checkout-step ${state}`}>
            <span className="step-circle" aria-hidden="true">
              <i className={step.icon}></i>
            </span>
            <div className="step-text">
              <div className="step-title">{step.title}</div>
              <div className="step-sub">{step.subtitle}</div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
