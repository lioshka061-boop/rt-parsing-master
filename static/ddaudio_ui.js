(() => {
	function parseNumberField(input) {
		if (!input) return null;
		const raw = input.value.trim();
		if (!raw) return null;
		const value = Number(raw);
		return Number.isNaN(value) ? null : value;
	}

	function numbersEqual(a, b) {
		if (a === null && b === null) return true;
		if (a === null || b === null) return false;
		return Math.abs(a - b) < 0.0001;
	}

	function getDefaultRule(form) {
		const priceType = form.querySelector('[name="ddaudio_default_price_type"]');
		const markup = form.querySelector('[name="ddaudio_default_markup"]');
		const discount = form.querySelector('[name="ddaudio_default_discount"]');
		const discountHours = form.querySelector('[name="ddaudio_default_discount_hours"]');
		const zeroStock = form.querySelector('[name="ddaudio_default_zero_stock"]');
		const roundTo9 = form.querySelector('[name="ddaudio_default_round_to_9"]');

		return {
			priceType: priceType ? priceType.value : '',
			markup: parseNumberField(markup),
			discount: parseNumberField(discount),
			discountHours: parseNumberField(discountHours),
			zeroStock: zeroStock ? zeroStock.value : '',
			roundTo9: roundTo9 ? roundTo9.checked : false,
		};
	}

	function getRuleFields(rule) {
		return {
			priceType: rule.querySelector('select[name^="cat_price_type_"], select[name^="sub_price_type_"]'),
			markup: rule.querySelector('input[name^="cat_markup_"], input[name^="sub_markup_"]'),
			discount: rule.querySelector('input[name^="cat_discount_"], input[name^="sub_discount_"]'),
			discountHours: rule.querySelector('input[name^="cat_discount_hours_"], input[name^="sub_discount_hours_"]'),
			zeroStock: rule.querySelector('select[name^="cat_zero_stock_"], select[name^="sub_zero_stock_"]'),
			roundTo9: rule.querySelector('input[name^="cat_round_to_9_"], input[name^="sub_round_to_9_"]'),
		};
	}

	function getRuleValues(rule) {
		const fields = getRuleFields(rule);
		return {
			priceType: fields.priceType ? fields.priceType.value : '',
			markup: parseNumberField(fields.markup),
			discount: parseNumberField(fields.discount),
			discountHours: parseNumberField(fields.discountHours),
			zeroStock: fields.zeroStock ? fields.zeroStock.value : '',
			roundTo9: fields.roundTo9 ? fields.roundTo9.checked : false,
		};
	}

	function ruleMatchesDefault(rule, defaults) {
		const values = getRuleValues(rule);
		return values.priceType === defaults.priceType
			&& numbersEqual(values.markup, defaults.markup)
			&& numbersEqual(values.discount, defaults.discount)
			&& numbersEqual(values.discountHours, defaults.discountHours)
			&& values.zeroStock === defaults.zeroStock
			&& values.roundTo9 === defaults.roundTo9;
	}

	function applyDefaults(rule, defaults) {
		const fields = getRuleFields(rule);
		if (fields.priceType) fields.priceType.value = defaults.priceType;
		if (fields.markup) fields.markup.value = defaults.markup ?? '';
		if (fields.discount) fields.discount.value = defaults.discount ?? '';
		if (fields.discountHours) fields.discountHours.value = defaults.discountHours ?? '';
		if (fields.zeroStock) fields.zeroStock.value = defaults.zeroStock;
		if (fields.roundTo9) fields.roundTo9.checked = defaults.roundTo9;
	}

	function updateRuleInheritState(rule, form) {
		const defaults = getDefaultRule(form);
		rule.dataset.ddaudioInherit = ruleMatchesDefault(rule, defaults) ? 'true' : 'false';
	}

	function syncInheritedRules(form) {
		const defaults = getDefaultRule(form);
		form.querySelectorAll('.ddaudio-category .ddaudio-rule').forEach((rule) => {
			if (rule.dataset.ddaudioInherit === 'true') {
				applyDefaults(rule, defaults);
			}
			rule.dataset.ddaudioInherit = ruleMatchesDefault(rule, defaults) ? 'true' : 'false';
		});
	}

	function initRuleInheritance(form) {
		const rules = form.querySelectorAll('.ddaudio-category .ddaudio-rule');
		if (!rules.length) return;

		rules.forEach((rule) => {
			updateRuleInheritState(rule, form);
			rule.addEventListener('input', () => updateRuleInheritState(rule, form));
			rule.addEventListener('change', () => updateRuleInheritState(rule, form));
		});

		form.querySelectorAll('[name^="ddaudio_default_"]').forEach((input) => {
			input.addEventListener('input', () => syncInheritedRules(form));
			input.addEventListener('change', () => syncInheritedRules(form));
		});
	}

	function initCategoryCheckboxes(form) {
		form.querySelectorAll('.ddaudio-category').forEach((category) => {
			const parent = category.querySelector('summary input[name="ddaudio_category"]');
			const children = Array.from(category.querySelectorAll('input[name="ddaudio_subcategory"]'));
			if (!parent || !children.length) return;

			const updateParent = () => {
				const checkedCount = children.filter((child) => child.checked).length;
				if (checkedCount === 0) {
					parent.checked = false;
					parent.indeterminate = false;
				} else if (checkedCount === children.length) {
					parent.checked = true;
					parent.indeterminate = false;
				} else {
					parent.checked = true;
					parent.indeterminate = true;
				}
			};

			parent.addEventListener('change', () => {
				const checked = parent.checked;
				parent.indeterminate = false;
				children.forEach((child) => {
					child.checked = checked;
				});
			});

			children.forEach((child) => {
				child.addEventListener('change', updateParent);
			});

			updateParent();
		});
	}

	function initForm(form) {
		if (!form.querySelector('[name="ddaudio_default_price_type"]')) return;
		initCategoryCheckboxes(form);
		initRuleInheritance(form);
	}

	function init() {
		document.querySelectorAll('form').forEach(initForm);
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', init);
	} else {
		init();
	}
})();
