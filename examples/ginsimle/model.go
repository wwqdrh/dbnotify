package main

type Company struct {
	ID      int `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	Name    string
	Age     int
	Address string
	Salary  int
}

type CompanyRela struct {
	ID        int `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	CompanyID int
	Salary    int
}
