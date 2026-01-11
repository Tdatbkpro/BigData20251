FROM nginx:alpine

# Install dependencies
RUN apk add --no-cache curl bash

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy custom nginx config
COPY nginx.conf /etc/nginx/nginx.conf

# Copy SSL certificates (if available)
# COPY ssl/ /etc/nginx/ssl/

# Create directories
RUN mkdir -p /var/log/nginx && \
    mkdir -p /var/cache/nginx && \
    chown -R nginx:nginx /var/log/nginx /var/cache/nginx

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost/health || exit 1

# Expose ports
EXPOSE 80 443

# Start nginx
CMD ["nginx", "-g", "daemon off;"]