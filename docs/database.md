# MySQL Database

Fully defined in `github/wtsi-hgi/lustre-usage-db-transfer`

### `pi`

| Field    | Type    | Constraints          |
| :--------| :-------| :--------------------|   
| pi_id    | int     | PK auto_increment    |
| pi_name  | text    |                      |

### `unix_group`

| Field     | Type    | Constraints          |
| :---------| :-------| :--------------------|   
| group_id  | int     | PK auto_increment    |
| group_name| text    |                      |
| is_humgen | boolean |                      |

### `volume`

| Field        | Type    | Constraints          |
| :------------| :-------| :--------------------|   
| volume_id    | int     | PK auto_increment    |
| scratch_disk | text    |                      |

### `lustre_usage`

| Field         | Type      | Constraints               |
| :-------------| :---------| :-------------------------|
| record_id     | int       | PK auto_increment         |
| used          | bigint(8) |                           |
| quota         | bigint(8) |                           |
| record_date   | date      |                           |
| archived      | boolean   |                           |
| last_modified | int       |                           |
| pi_id         | int       | FK `pi.pi_id`             |
| unix_id       | int       | FK `unix_group.group_id`  |
| volume_id     | int       | FK `volume.volume_id`     |

### `directory`

| Field          | Type      | Constraints              |
| :--------------| :---------| :------------------------|
| directory_id   | int       | PK auto_increment        |
| project_name   | text      |                          |
| directory_path | text      |                          |
| num_files      | bigint(8) |                          |
| size           | bigint(8) |                          |
| last_modified  | int       |                          |
| pi_id          | int       | FK `pi.pi_id`            |
| group_id       | int       | FK `unix_group.group_id` |
| volume_id      | int       | FK `volume.volume_id`    |

### `filetype`

| Field         | Type | Constraints       |
| :-------------| :----| :-----------------|
| filetype_id   | int  | PK auto_increment |
| filetype_name | text |                   |

Filled out to:
| filetype_id | filetype_name |
| :-----------| :-------------|
| 1           | BAM           |
| 2           | CRAM          |
| 3           | VCF           |
| 4           | PEDBED        |

### `file_size`

| Field        | Type  | Constraints                 |
| :------------| :-----| :---------------------------|
| file_size_id | int   | PK auto_increment           |
| directory_id | int   | FK `directory.directory_id` |
| filetype_id  | int   | FK `filetype.filetype_id`   |
| size         | float |                             |

### `vault_actions`

| Field           | Type | Constraints       |
| :---------------| :----| :-----------------|
| vault_action_id | int  | PK auto_increment |
| action_name     | text |                   |

Filled out to:
| vault_action_id | action_name |
| :---------------| :-----------|
| 1               | Keep        |
| 2               | Archive     |

### vault

| Field           | Type | Constraints                        |
| :---------------| :----| :----------------------------------|
| record_id       | int  | PK auto_increment                  |
| record_date     | date |                                    |
| filepath        | text |                                    |
| group_id        | int  | FK `group.group_id`                |
| vault_action_id | int  | FK `vault_actions.vault_action_id` |
| size            | int  |                                    |
| file_owner      | text |                                    |
| last_modified   | date |                                    |
| volume_id       | int  | FK `volume.volume_id`              |