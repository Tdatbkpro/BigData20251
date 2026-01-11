# =====================
# Build stage
# =====================
FROM node:18-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci --legacy-peer-deps

COPY . .
RUN npm run build


# =====================
# Artifact stage
# =====================
FROM alpine:3.18

WORKDIR /frontend

COPY --from=build /app/build ./build

# Container này chỉ giữ build output
CMD ["sh", "-c", "sleep infinity"]
