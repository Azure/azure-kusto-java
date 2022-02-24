package com.microsoft.azure.kusto.ingest.result;

import java.io.Serializable;

public class ValidationPolicy implements Serializable {
    private ValidationOptions validationOptions = ValidationOptions.DoNotValidate;
    private ValidationImplications validationPolicyType = ValidationImplications.BestEffort;

    public ValidationPolicy() {
    }

    public ValidationPolicy(ValidationOptions validationOptions, ValidationImplications validationPolicyType) {
        this.validationOptions = validationOptions;
        this.validationPolicyType = validationPolicyType;
    }

    public ValidationPolicy(ValidationPolicy other) {
        this.validationOptions = other.validationOptions;
        this.validationPolicyType = other.validationPolicyType;
    }

    public ValidationOptions getValidationOptions() {
        return validationOptions;
    }

    public void setValidationOptions(ValidationOptions validationOptions) {
        this.validationOptions = validationOptions;
    }

    public ValidationImplications getValidationPolicyType() {
        return validationPolicyType;
    }

    public void setValidationPolicyType(ValidationImplications validationPolicyType) {
        this.validationPolicyType = validationPolicyType;
    }

    public enum ValidationOptions {
        DoNotValidate("DoNotValidate"),
        ValidateCsvInputConstantColumns("ValidateCsvInputConstantColumns"),
        ValidateCsvInputColumnLevelOnly("ValidateCsvInputColumnLevelOnly");

        private final String kustoValue;

        ValidationOptions(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        public String getKustoValue() {
            return kustoValue;
        }
    }

    public enum ValidationImplications {
        Fail("Fail"),
        BestEffort("BestEffort");

        private final String kustoValue;

        ValidationImplications(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        public String getKustoValue() {
            return kustoValue;
        }
    }
}
