package com.java26groupwork.finalassignment.api;

import org.springframework.boot.autoconfigure.web.servlet.MultipartProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.server.ResponseStatusException;

@RestControllerAdvice
public class ApiExceptionHandler {

    private final MultipartProperties multipartProperties;

    public ApiExceptionHandler(MultipartProperties multipartProperties) {
        this.multipartProperties = multipartProperties;
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ApiErrorResponse> handleMaxUploadSizeExceeded(MaxUploadSizeExceededException exception) {
        String limitText = formatLimit(multipartProperties.getMaxRequestSize().toMegabytes());
        String message = "Upload too large. The current dataset upload limit is "
                + limitText
                + " per request. Split the dataset into smaller files or upload fewer files at once.";
        return buildResponse(HttpStatus.PAYLOAD_TOO_LARGE, message);
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiErrorResponse> handleResponseStatus(ResponseStatusException exception) {
        HttpStatus status = HttpStatus.valueOf(exception.getStatusCode().value());
        String message = exception.getReason() == null || exception.getReason().isBlank()
                ? status.getReasonPhrase()
                : exception.getReason();
        return buildResponse(status, message);
    }

    private ResponseEntity<ApiErrorResponse> buildResponse(HttpStatus status, String message) {
        return ResponseEntity.status(status)
                .body(new ApiErrorResponse(status.value(), status.getReasonPhrase(), message));
    }

    private String formatLimit(long megabytes) {
        if (megabytes <= 0) {
            return "the configured size limit";
        }
        if (megabytes % 1024 == 0) {
            return (megabytes / 1024) + " GB";
        }
        return megabytes + " MB";
    }

    public record ApiErrorResponse(int status, String error, String message) {
    }
}
