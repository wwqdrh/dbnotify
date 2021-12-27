package op

import "testing"

func TestInsertSql(t *testing.T) {
	_, err := NewBaseOperator(`
	INSERT INTO COMPANY (name, age, address, salary) VALUES
('Mark', 25, 'Rich-Mond ', 65000.00 ),
('David', 27, 'Texas', 85000.00);
	`)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateSql(t *testing.T) {
	op, err := NewUpdateOperator(`
	UPDATE "company" SET "name"="new_name" WHERE id=1
	`)
	if err != nil {
		t.Error(err)
	}
	op.Initatial()

	// test reverse
	if stmt := op.ReverSQL(); stmt != `UPDATE "company" SET "name"="mock_old_value" WHERE id=1` {
		t.Error("update reverse error: ", stmt)
	}
}

func TestDeleteSql(t *testing.T) {
	_, err := NewBaseOperator(`
	DELETE FROM table_name WHERE some_column=some_value
	`)
	if err != nil {
		t.Error(err)
	}
}
