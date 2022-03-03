package com.microsoft.azure.kusto.ingest.result;

import com.fasterxml.jackson.annotation.JsonValue;
import java.io.Serializable;

public class ValidationPolicy implements Serializable {
    private ValidationOptions validationOptions = ValidationOptions.DO_NOT_VALIDATE;
    private ValidationImplications validationPolicyType = ValidationImplications.VALIDATION_IMPLICATIONS;

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
        DO_NOT_VALIDATE("DoNotValidate"),
        VALIDATE_CSV_INPUT_CONSTANT_COLUMNS("ValidateCsvInputConstantColumns"),
        VALIDATE_CSV_INPUT_COLUMN_LEVEL_ONLY("ValidateCsvInputColumnLevelOnly");

        private final String kustoValue;

        ValidationOptions(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        @JsonValue
        public String getKustoValue() {
            return kustoValue;
        }
    }

    public enum ValidationImplications {
        FAIL("Fail"),
        VALIDATION_IMPLICATIONS("BestEffort");

        private final String kustoValue;

        ValidationImplications(String kustoValue) {
            this.kustoValue = kustoValue;
        }

        @JsonValue
        public String getKustoValue() {
            return kustoValue;
        }
    }
}
