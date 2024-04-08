-- +goose Up
-- +goose StatementBegin
create table hello (id integer);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
drop table hello;
-- +goose StatementEnd
