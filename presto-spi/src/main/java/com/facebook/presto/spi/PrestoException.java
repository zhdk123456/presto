/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.PrintWriter;
import java.io.StringWriter;

public class PrestoException
        extends RuntimeException
{
    private final ErrorCode errorCode;

    public PrestoException(ErrorCodeSupplier errorCode, String message)
    {
        this(errorCode, message, null);
    }

    public PrestoException(ErrorCodeSupplier errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public PrestoException(ErrorCodeSupplier errorCodeSupplier, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public PrestoException(ErrorCodeSupplier errorCodeSupplier, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public PrestoExceptionSerialized toSerialized()
    {
        return new PrestoExceptionSerialized(errorCode, getMessage(), getStackTraceString());
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }

    private String getStackTraceString()
    {
        StringWriter writer = new StringWriter();
        printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    public static class PrestoExceptionSerialized
    {
        private final ErrorCode errorCode;
        private final String message;
        private final String stackTraceString;

        @JsonCreator
        public PrestoExceptionSerialized(
                @JsonProperty("errorCode") ErrorCode errorCode,
                @JsonProperty("message") String message,
                @JsonProperty("stackTraceString") String stackTraceString)
        {
            this.errorCode = errorCode;
            this.message = message;
            this.stackTraceString = stackTraceString;
        }

        @JsonProperty
        public ErrorCode getErrorCode()
        {
            return errorCode;
        }

        @JsonProperty
        public String getMessage()
        {
            return message;
        }

        @JsonProperty
        public String getStackTraceString()
        {
            return stackTraceString;
        }
    }
}
