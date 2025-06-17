class SensitivityHandler:
    """Handle sensitive data with configurable masking options (backward compatible)"""

    def __init__(self, enable_masking=False, masking_config=None):
        self.enable_masking = enable_masking
        self.masking_config = masking_config or {}

        # Default masking patterns (can be overridden)
        self.default_patterns = {
            "PII": {
                "email": lambda x: self._mask_email(x),
                "phone": lambda x: self._mask_phone(x),
                "name": lambda x: self._mask_name(x),
                "default": lambda x: self._mask_generic_pii(x)
            },
            "FINANCIAL": {
                "income": lambda x: self._mask_financial_amount(x),
                "salary": lambda x: self._mask_financial_amount(x),
                "default": lambda x: self._mask_financial_generic(x)
            }
        }

    def apply_sensitivity_handling(self, value, column_name, sensitivity_level, data_type):
        """Apply sensitivity handling - masking disabled by default for backward compatibility"""

        # Backward compatibility: if masking disabled, return original value
        if not self.enable_masking:
            return value

        if not sensitivity_level or value is None:
            return value

        sensitivity_upper = sensitivity_level.upper()
        column_lower = column_name.lower()

        # Get masking function
        if sensitivity_upper in self.default_patterns:
            patterns = self.default_patterns[sensitivity_upper]

            # Find specific pattern for column type
            masking_func = None
            for pattern_key, func in patterns.items():
                if pattern_key in column_lower:
                    masking_func = func
                    break

            # Use default pattern if no specific match
            if not masking_func:
                masking_func = patterns.get("default", lambda x: x)

            return masking_func(value)

        return value

    def _mask_email(self, email):
        """Mask email while preserving format"""
        if not isinstance(email, str) or '@' not in email:
            return email

        local, domain = email.split('@', 1)
        if len(local) > 2:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        else:
            masked_local = '*' * len(local)

        return f"{masked_local}@{domain}"

    def _mask_phone(self, phone):
        """Mask phone number preserving format"""
        if not isinstance(phone, str):
            return phone

        # Remove non-digits for processing
        digits_only = ''.join(filter(str.isdigit, phone))

        if len(digits_only) >= 4:
            # Mask middle digits
            visible_start = min(2, len(digits_only) // 3)
            visible_end = min(2, len(digits_only) // 3)
            masked_middle = '*' * (len(digits_only) - visible_start - visible_end)
            masked_digits = digits_only[:visible_start] + masked_middle + digits_only[-visible_end:]

            # Preserve original format
            result = phone
            for original, masked in zip(digits_only, masked_digits):
                result = result.replace(original, masked, 1)
            return result

        return phone

    def _mask_name(self, name):
        """Mask name preserving first letter"""
        if not isinstance(name, str) or len(name) <= 1:
            return name

        return name[0] + '*' * (len(name) - 1)

    def _mask_generic_pii(self, value):
        """Generic PII masking"""
        if not isinstance(value, str):
            return value

        if len(value) <= 2:
            return '*' * len(value)
        else:
            return value[0] + '*' * (len(value) - 2) + value[-1]

    def _mask_financial_amount(self, amount):
        """Mask financial amounts by ranges"""
        if isinstance(amount, (int, float)):
            if amount < 1000:
                return "< 1K"
            elif amount < 10000:
                return "1K - 10K"
            elif amount < 50000:
                return "10K - 50K"
            elif amount < 100000:
                return "50K - 100K"
            else:
                return "> 100K"
        return amount

    def _mask_financial_generic(self, value):
        """Generic financial data masking"""
        return "[FINANCIAL_DATA]"
