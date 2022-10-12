package jsondb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	_, err := Open("./storege")
	assert.Nil(t, err)
}

type Student struct {
	Model
	Name string
	Sex  string
}

func (s Student) TableName() string {
	return "student"
}

func TestCreate(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		t.Error(err)
	}

	err = db.Model(Student{}).Create(Student{
		Name: "xiehengjian",
		Sex:  "male",
	})
	if err != nil {
		t.Error(err)
	}
	if _, err := os.Stat("./test/Customer"); err != nil {
		t.Errorf("Failed to create customer db file")
	}
}

func TestFind(t *testing.T) {
	db, err := Open("test")
	if err != nil {
		t.Error(err)
	}
	db.Model(Student{}).Where("").
}
