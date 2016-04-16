package sqlcache

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"

	"gopkg.in/redis.v3"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SQLCache struct {
	dbConn   *sqlx.DB
	redisCli *redis.Client
}

type PGConfig struct {
	Host     string
	Port     string
	User     string
	Pass     string
	Database string
}
type RdsConfig struct {
	Host     string
	Port     string
	Pass     string
	Database int64
}

func (c *PGConfig) SetDefaults() {
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == "" {
		c.Port = "5432"
	}
	if c.User == "" {
		c.User = "postgres"
	}
}
func (c *RdsConfig) SetDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == "" {
		c.Port = "6379"
	}
	if c.Database == 0 {
		c.Database = 0
	}
}

func (c *PGConfig) SelectDB(db string) {
	c.Database = db
}

func New(cfg *PGConfig, rcfg *RdsConfig) *SQLCache {
	cfg.SetDefaults()
	rcfg.SetDefaults()

	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Pass, cfg.Database)
	db, err := sqlx.Open("postgres", connectionString)
	if err != nil {
		log.Fatalln("failed to open Postgre", err, connectionString)
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", rcfg.Host, rcfg.Port),
		Password: rcfg.Pass,     // no password set
		DB:       rcfg.Database, // use default DB
	})
	_, err = client.Ping().Result()
	if err != nil {
		log.Fatalln("failed to open Redis", err)
	}

	return &SQLCache{dbConn: db, redisCli: client}
}

func getCache(key string) []map[string]interface{} {
	return make([]map[string]interface{}, 0)
}
func checkCache(key string) bool {
	return true
}
func setCache(key string, val string) bool {
	return true
}

func (s *SQLCache) QueryAndCache(q string) ([]map[string]interface{}, error) {
	var err error
	key := md5.Sum([]byte(q))
	bytes.IndexByte(byteArray, 0)
	if checkCache(key) {
		return getCache(key), err
	}

	result := make([]map[string]interface{}, 0)
	rows, err := s.dbConn.Queryx(q)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		e := make(map[string]interface{})
		rows.MapScan(e)
		result = append(result, e)
	}
	resString, _ := json.Marshal(result)
	setCache(key, resString)

	return result, err
}
