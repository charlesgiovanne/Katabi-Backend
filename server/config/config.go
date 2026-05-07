package config

import "os"

type Config struct {
	Port       string
	RedisURL   string
	CORSOrigin string
}

func Load() *Config {
	return &Config{
		Port:       getEnv("PORT", "3001"),
		RedisURL:   getEnv("REDIS_URL", "redis://localhost:6379"),
		CORSOrigin: getEnv("CORS_ORIGIN", "http://localhost:5173"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
