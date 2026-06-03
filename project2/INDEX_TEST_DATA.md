# Index Test Data and Validation Guide

This guide provides copy-paste SQL scripts for testing the in-memory B+ tree index implementation.

Run from `project2` with `edu.sustech.cs307.DBEntry`, or execute the same SQL through the existing planner/operator test helper.

Important: `DBEntry` reads one line at a time. Each SQL statement below must be pasted as one complete line. Do not split one `insert ... values ...;` statement across multiple lines in the interactive shell.

## Goal

These scripts cover:

- Large enough datasets for index stress testing.
- `create index idx on t(col)` and `drop index idx`.
- Multiple indexes on one table.
- Equality lookup.
- Range lookup.
- Duplicate keys.
- Dynamic index maintenance after `insert`, `delete`, and `update`.
- Planner fallback after `drop index`.

## Dataset A: 1000 Rows, Multiple Indexes

This dataset is intentionally much larger than one record page and is enough to trigger many B+ tree node splits with the current default B+ tree order.

```sql
create table t(id int, age int);

insert into t (id, age) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 0);
insert into t (id, age) values (11, 1), (12, 2), (13, 3), (14, 4), (15, 5), (16, 6), (17, 7), (18, 8), (19, 9), (20, 0);
insert into t (id, age) values (21, 1), (22, 2), (23, 3), (24, 4), (25, 5), (26, 6), (27, 7), (28, 8), (29, 9), (30, 0);
insert into t (id, age) values (31, 1), (32, 2), (33, 3), (34, 4), (35, 5), (36, 6), (37, 7), (38, 8), (39, 9), (40, 0);
insert into t (id, age) values (41, 1), (42, 2), (43, 3), (44, 4), (45, 5), (46, 6), (47, 7), (48, 8), (49, 9), (50, 0);
insert into t (id, age) values (51, 1), (52, 2), (53, 3), (54, 4), (55, 5), (56, 6), (57, 7), (58, 8), (59, 9), (60, 0);
insert into t (id, age) values (61, 1), (62, 2), (63, 3), (64, 4), (65, 5), (66, 6), (67, 7), (68, 8), (69, 9), (70, 0);
insert into t (id, age) values (71, 1), (72, 2), (73, 3), (74, 4), (75, 5), (76, 6), (77, 7), (78, 8), (79, 9), (80, 0);
insert into t (id, age) values (81, 1), (82, 2), (83, 3), (84, 4), (85, 5), (86, 6), (87, 7), (88, 8), (89, 9), (90, 0);
insert into t (id, age) values (91, 1), (92, 2), (93, 3), (94, 4), (95, 5), (96, 6), (97, 7), (98, 8), (99, 9), (100, 0);
insert into t (id, age) values (101, 1), (102, 2), (103, 3), (104, 4), (105, 5), (106, 6), (107, 7), (108, 8), (109, 9), (110, 0);
insert into t (id, age) values (111, 1), (112, 2), (113, 3), (114, 4), (115, 5), (116, 6), (117, 7), (118, 8), (119, 9), (120, 0);
insert into t (id, age) values (121, 1), (122, 2), (123, 3), (124, 4), (125, 5), (126, 6), (127, 7), (128, 8), (129, 9), (130, 0);
insert into t (id, age) values (131, 1), (132, 2), (133, 3), (134, 4), (135, 5), (136, 6), (137, 7), (138, 8), (139, 9), (140, 0);
insert into t (id, age) values (141, 1), (142, 2), (143, 3), (144, 4), (145, 5), (146, 6), (147, 7), (148, 8), (149, 9), (150, 0);
insert into t (id, age) values (151, 1), (152, 2), (153, 3), (154, 4), (155, 5), (156, 6), (157, 7), (158, 8), (159, 9), (160, 0);
insert into t (id, age) values (161, 1), (162, 2), (163, 3), (164, 4), (165, 5), (166, 6), (167, 7), (168, 8), (169, 9), (170, 0);
insert into t (id, age) values (171, 1), (172, 2), (173, 3), (174, 4), (175, 5), (176, 6), (177, 7), (178, 8), (179, 9), (180, 0);
insert into t (id, age) values (181, 1), (182, 2), (183, 3), (184, 4), (185, 5), (186, 6), (187, 7), (188, 8), (189, 9), (190, 0);
insert into t (id, age) values (191, 1), (192, 2), (193, 3), (194, 4), (195, 5), (196, 6), (197, 7), (198, 8), (199, 9), (200, 0);
insert into t (id, age) values (201, 1), (202, 2), (203, 3), (204, 4), (205, 5), (206, 6), (207, 7), (208, 8), (209, 9), (210, 0);
insert into t (id, age) values (211, 1), (212, 2), (213, 3), (214, 4), (215, 5), (216, 6), (217, 7), (218, 8), (219, 9), (220, 0);
insert into t (id, age) values (221, 1), (222, 2), (223, 3), (224, 4), (225, 5), (226, 6), (227, 7), (228, 8), (229, 9), (230, 0);
insert into t (id, age) values (231, 1), (232, 2), (233, 3), (234, 4), (235, 5), (236, 6), (237, 7), (238, 8), (239, 9), (240, 0);
insert into t (id, age) values (241, 1), (242, 2), (243, 3), (244, 4), (245, 5), (246, 6), (247, 7), (248, 8), (249, 9), (250, 0);
insert into t (id, age) values (251, 1), (252, 2), (253, 3), (254, 4), (255, 5), (256, 6), (257, 7), (258, 8), (259, 9), (260, 0);
insert into t (id, age) values (261, 1), (262, 2), (263, 3), (264, 4), (265, 5), (266, 6), (267, 7), (268, 8), (269, 9), (270, 0);
insert into t (id, age) values (271, 1), (272, 2), (273, 3), (274, 4), (275, 5), (276, 6), (277, 7), (278, 8), (279, 9), (280, 0);
insert into t (id, age) values (281, 1), (282, 2), (283, 3), (284, 4), (285, 5), (286, 6), (287, 7), (288, 8), (289, 9), (290, 0);
insert into t (id, age) values (291, 1), (292, 2), (293, 3), (294, 4), (295, 5), (296, 6), (297, 7), (298, 8), (299, 9), (300, 0);
insert into t (id, age) values (301, 1), (302, 2), (303, 3), (304, 4), (305, 5), (306, 6), (307, 7), (308, 8), (309, 9), (310, 0);
insert into t (id, age) values (311, 1), (312, 2), (313, 3), (314, 4), (315, 5), (316, 6), (317, 7), (318, 8), (319, 9), (320, 0);
insert into t (id, age) values (321, 1), (322, 2), (323, 3), (324, 4), (325, 5), (326, 6), (327, 7), (328, 8), (329, 9), (330, 0);
insert into t (id, age) values (331, 1), (332, 2), (333, 3), (334, 4), (335, 5), (336, 6), (337, 7), (338, 8), (339, 9), (340, 0);
insert into t (id, age) values (341, 1), (342, 2), (343, 3), (344, 4), (345, 5), (346, 6), (347, 7), (348, 8), (349, 9), (350, 0);
insert into t (id, age) values (351, 1), (352, 2), (353, 3), (354, 4), (355, 5), (356, 6), (357, 7), (358, 8), (359, 9), (360, 0);
insert into t (id, age) values (361, 1), (362, 2), (363, 3), (364, 4), (365, 5), (366, 6), (367, 7), (368, 8), (369, 9), (370, 0);
insert into t (id, age) values (371, 1), (372, 2), (373, 3), (374, 4), (375, 5), (376, 6), (377, 7), (378, 8), (379, 9), (380, 0);
insert into t (id, age) values (381, 1), (382, 2), (383, 3), (384, 4), (385, 5), (386, 6), (387, 7), (388, 8), (389, 9), (390, 0);
insert into t (id, age) values (391, 1), (392, 2), (393, 3), (394, 4), (395, 5), (396, 6), (397, 7), (398, 8), (399, 9), (400, 0);
insert into t (id, age) values (401, 1), (402, 2), (403, 3), (404, 4), (405, 5), (406, 6), (407, 7), (408, 8), (409, 9), (410, 0);
insert into t (id, age) values (411, 1), (412, 2), (413, 3), (414, 4), (415, 5), (416, 6), (417, 7), (418, 8), (419, 9), (420, 0);
insert into t (id, age) values (421, 1), (422, 2), (423, 3), (424, 4), (425, 5), (426, 6), (427, 7), (428, 8), (429, 9), (430, 0);
insert into t (id, age) values (431, 1), (432, 2), (433, 3), (434, 4), (435, 5), (436, 6), (437, 7), (438, 8), (439, 9), (440, 0);
insert into t (id, age) values (441, 1), (442, 2), (443, 3), (444, 4), (445, 5), (446, 6), (447, 7), (448, 8), (449, 9), (450, 0);
insert into t (id, age) values (451, 1), (452, 2), (453, 3), (454, 4), (455, 5), (456, 6), (457, 7), (458, 8), (459, 9), (460, 0);
insert into t (id, age) values (461, 1), (462, 2), (463, 3), (464, 4), (465, 5), (466, 6), (467, 7), (468, 8), (469, 9), (470, 0);
insert into t (id, age) values (471, 1), (472, 2), (473, 3), (474, 4), (475, 5), (476, 6), (477, 7), (478, 8), (479, 9), (480, 0);
insert into t (id, age) values (481, 1), (482, 2), (483, 3), (484, 4), (485, 5), (486, 6), (487, 7), (488, 8), (489, 9), (490, 0);
insert into t (id, age) values (491, 1), (492, 2), (493, 3), (494, 4), (495, 5), (496, 6), (497, 7), (498, 8), (499, 9), (500, 0);
insert into t (id, age) values (501, 1), (502, 2), (503, 3), (504, 4), (505, 5), (506, 6), (507, 7), (508, 8), (509, 9), (510, 0);
insert into t (id, age) values (511, 1), (512, 2), (513, 3), (514, 4), (515, 5), (516, 6), (517, 7), (518, 8), (519, 9), (520, 0);
insert into t (id, age) values (521, 1), (522, 2), (523, 3), (524, 4), (525, 5), (526, 6), (527, 7), (528, 8), (529, 9), (530, 0);
insert into t (id, age) values (531, 1), (532, 2), (533, 3), (534, 4), (535, 5), (536, 6), (537, 7), (538, 8), (539, 9), (540, 0);
insert into t (id, age) values (541, 1), (542, 2), (543, 3), (544, 4), (545, 5), (546, 6), (547, 7), (548, 8), (549, 9), (550, 0);
insert into t (id, age) values (551, 1), (552, 2), (553, 3), (554, 4), (555, 5), (556, 6), (557, 7), (558, 8), (559, 9), (560, 0);
insert into t (id, age) values (561, 1), (562, 2), (563, 3), (564, 4), (565, 5), (566, 6), (567, 7), (568, 8), (569, 9), (570, 0);
insert into t (id, age) values (571, 1), (572, 2), (573, 3), (574, 4), (575, 5), (576, 6), (577, 7), (578, 8), (579, 9), (580, 0);
insert into t (id, age) values (581, 1), (582, 2), (583, 3), (584, 4), (585, 5), (586, 6), (587, 7), (588, 8), (589, 9), (590, 0);
insert into t (id, age) values (591, 1), (592, 2), (593, 3), (594, 4), (595, 5), (596, 6), (597, 7), (598, 8), (599, 9), (600, 0);
insert into t (id, age) values (601, 1), (602, 2), (603, 3), (604, 4), (605, 5), (606, 6), (607, 7), (608, 8), (609, 9), (610, 0);
insert into t (id, age) values (611, 1), (612, 2), (613, 3), (614, 4), (615, 5), (616, 6), (617, 7), (618, 8), (619, 9), (620, 0);
insert into t (id, age) values (621, 1), (622, 2), (623, 3), (624, 4), (625, 5), (626, 6), (627, 7), (628, 8), (629, 9), (630, 0);
insert into t (id, age) values (631, 1), (632, 2), (633, 3), (634, 4), (635, 5), (636, 6), (637, 7), (638, 8), (639, 9), (640, 0);
insert into t (id, age) values (641, 1), (642, 2), (643, 3), (644, 4), (645, 5), (646, 6), (647, 7), (648, 8), (649, 9), (650, 0);
insert into t (id, age) values (651, 1), (652, 2), (653, 3), (654, 4), (655, 5), (656, 6), (657, 7), (658, 8), (659, 9), (660, 0);
insert into t (id, age) values (661, 1), (662, 2), (663, 3), (664, 4), (665, 5), (666, 6), (667, 7), (668, 8), (669, 9), (670, 0);
insert into t (id, age) values (671, 1), (672, 2), (673, 3), (674, 4), (675, 5), (676, 6), (677, 7), (678, 8), (679, 9), (680, 0);
insert into t (id, age) values (681, 1), (682, 2), (683, 3), (684, 4), (685, 5), (686, 6), (687, 7), (688, 8), (689, 9), (690, 0);
insert into t (id, age) values (691, 1), (692, 2), (693, 3), (694, 4), (695, 5), (696, 6), (697, 7), (698, 8), (699, 9), (700, 0);
insert into t (id, age) values (701, 1), (702, 2), (703, 3), (704, 4), (705, 5), (706, 6), (707, 7), (708, 8), (709, 9), (710, 0);
insert into t (id, age) values (711, 1), (712, 2), (713, 3), (714, 4), (715, 5), (716, 6), (717, 7), (718, 8), (719, 9), (720, 0);
insert into t (id, age) values (721, 1), (722, 2), (723, 3), (724, 4), (725, 5), (726, 6), (727, 7), (728, 8), (729, 9), (730, 0);
insert into t (id, age) values (731, 1), (732, 2), (733, 3), (734, 4), (735, 5), (736, 6), (737, 7), (738, 8), (739, 9), (740, 0);
insert into t (id, age) values (741, 1), (742, 2), (743, 3), (744, 4), (745, 5), (746, 6), (747, 7), (748, 8), (749, 9), (750, 0);
insert into t (id, age) values (751, 1), (752, 2), (753, 3), (754, 4), (755, 5), (756, 6), (757, 7), (758, 8), (759, 9), (760, 0);
insert into t (id, age) values (761, 1), (762, 2), (763, 3), (764, 4), (765, 5), (766, 6), (767, 7), (768, 8), (769, 9), (770, 0);
insert into t (id, age) values (771, 1), (772, 2), (773, 3), (774, 4), (775, 5), (776, 6), (777, 7), (778, 8), (779, 9), (780, 0);
insert into t (id, age) values (781, 1), (782, 2), (783, 3), (784, 4), (785, 5), (786, 6), (787, 7), (788, 8), (789, 9), (790, 0);
insert into t (id, age) values (791, 1), (792, 2), (793, 3), (794, 4), (795, 5), (796, 6), (797, 7), (798, 8), (799, 9), (800, 0);
insert into t (id, age) values (801, 1), (802, 2), (803, 3), (804, 4), (805, 5), (806, 6), (807, 7), (808, 8), (809, 9), (810, 0);
insert into t (id, age) values (811, 1), (812, 2), (813, 3), (814, 4), (815, 5), (816, 6), (817, 7), (818, 8), (819, 9), (820, 0);
insert into t (id, age) values (821, 1), (822, 2), (823, 3), (824, 4), (825, 5), (826, 6), (827, 7), (828, 8), (829, 9), (830, 0);
insert into t (id, age) values (831, 1), (832, 2), (833, 3), (834, 4), (835, 5), (836, 6), (837, 7), (838, 8), (839, 9), (840, 0);
insert into t (id, age) values (841, 1), (842, 2), (843, 3), (844, 4), (845, 5), (846, 6), (847, 7), (848, 8), (849, 9), (850, 0);
insert into t (id, age) values (851, 1), (852, 2), (853, 3), (854, 4), (855, 5), (856, 6), (857, 7), (858, 8), (859, 9), (860, 0);
insert into t (id, age) values (861, 1), (862, 2), (863, 3), (864, 4), (865, 5), (866, 6), (867, 7), (868, 8), (869, 9), (870, 0);
insert into t (id, age) values (871, 1), (872, 2), (873, 3), (874, 4), (875, 5), (876, 6), (877, 7), (878, 8), (879, 9), (880, 0);
insert into t (id, age) values (881, 1), (882, 2), (883, 3), (884, 4), (885, 5), (886, 6), (887, 7), (888, 8), (889, 9), (890, 0);
insert into t (id, age) values (891, 1), (892, 2), (893, 3), (894, 4), (895, 5), (896, 6), (897, 7), (898, 8), (899, 9), (900, 0);
insert into t (id, age) values (901, 1), (902, 2), (903, 3), (904, 4), (905, 5), (906, 6), (907, 7), (908, 8), (909, 9), (910, 0);
insert into t (id, age) values (911, 1), (912, 2), (913, 3), (914, 4), (915, 5), (916, 6), (917, 7), (918, 8), (919, 9), (920, 0);
insert into t (id, age) values (921, 1), (922, 2), (923, 3), (924, 4), (925, 5), (926, 6), (927, 7), (928, 8), (929, 9), (930, 0);
insert into t (id, age) values (931, 1), (932, 2), (933, 3), (934, 4), (935, 5), (936, 6), (937, 7), (938, 8), (939, 9), (940, 0);
insert into t (id, age) values (941, 1), (942, 2), (943, 3), (944, 4), (945, 5), (946, 6), (947, 7), (948, 8), (949, 9), (950, 0);
insert into t (id, age) values (951, 1), (952, 2), (953, 3), (954, 4), (955, 5), (956, 6), (957, 7), (958, 8), (959, 9), (960, 0);
insert into t (id, age) values (961, 1), (962, 2), (963, 3), (964, 4), (965, 5), (966, 6), (967, 7), (968, 8), (969, 9), (970, 0);
insert into t (id, age) values (971, 1), (972, 2), (973, 3), (974, 4), (975, 5), (976, 6), (977, 7), (978, 8), (979, 9), (980, 0);
insert into t (id, age) values (981, 1), (982, 2), (983, 3), (984, 4), (985, 5), (986, 6), (987, 7), (988, 8), (989, 9), (990, 0);
insert into t (id, age) values (991, 1), (992, 2), (993, 3), (994, 4), (995, 5), (996, 6), (997, 7), (998, 8), (999, 9), (1000, 0);

create index idx_id on t(id);
create index idx_age on t(age);
```

Expected behavior:

- `idx_id` and `idx_age` are both written to metadata.
- Two in-memory B+ trees are created during this run.
- Creating each index prints the B+ tree by level.

## Equality Lookup

```sql
select * from t where t.id = 42;
```

Expected result:

```text
id = 42, age = 2
```

Why this matters:

- The optimizer should choose `IndexScanOperator` for `t.id = 42`.
- The B+ tree returns the RID for key `42`.
- `IndexScanOperator` uses the RID to read the real record from `t/data`.

## Range Lookup

```sql
select * from t where t.id >= 998;
select * from t where t.id < 4;
select * from t where t.id >= 10 and t.id < 15;
```

Expected first-column results:

```text
t.id >= 998      -> 998, 999, 1000
t.id < 4         -> 1, 2, 3
t.id >= 10 < 15  -> 10, 11, 12, 13, 14
```

Why this matters:

- Tests `moreThan`, `lessThan`, and `range` paths in the B+ tree.
- The third query should exercise merged range predicates when the optimizer detects both bounds on the same indexed column.

## Duplicate Key Lookup

```sql
select t.id from t where t.age = 1 order by t.id;
```

Expected result:

```text
All ids whose age is 1:
1, 11, 21, ..., 981, 991
```

Why this matters:

- `idx_age` maps one key to multiple RIDs.
- Leaf nodes store `ArrayList<RID>` per key, so duplicate key support is tested.

## Dynamic Insert Maintenance

```sql
insert into t (id, age) values (1001, 1);

select * from t where t.id = 1001;
select t.id from t where t.age = 1 order by t.id;
```

Expected result:

```text
t.id = 1001 -> one row: id = 1001, age = 1
t.age = 1   -> includes 1001
```

Why this matters:

- `InsertOperator` writes the new record and calls `DBManager.insertIndexEntries`.
- Both `idx_id` and `idx_age` should be updated in memory.

## Dynamic Delete Maintenance

```sql
delete from t where t.id = 42;

select * from t where t.id = 42;
select count(*) from t;
```

Expected result:

```text
t.id = 42 -> empty
count(*)  -> 1000
```

Explanation:

- Dataset A has 1000 rows.
- The insert of row 1001 increases the count to 1001.
- Deleting id 42 decreases the count to 1000.

Why this matters:

- `DeleteOperator` calls `RecordFileHandle.DeleteRecord`.
- Then it calls `DBManager.deleteIndexEntries`.
- The B+ tree must remove key `42 -> RID`.

## Dynamic Update Maintenance

```sql
update t set t.id = 142 where t.id = 43;

select * from t where t.id = 43;
select * from t where t.id = 142;
```

Expected result:

```text
t.id = 43  -> empty
t.id = 142 -> one row
```

Why this matters:

- `UpdateOperator` rewrites the record at the same RID.
- `DBManager.updateIndexEntries` deletes the old key and inserts the new key.

## Drop Index Fallback

```sql
drop index idx_id;

select * from t where t.id = 142;
```

Expected result:

```text
t.id = 142 -> one row
```

Why this matters:

- Metadata should no longer contain `idx_id`.
- Runtime index map should remove `idx_id`.
- The query should still be correct by falling back to sequential scan plus filter.

## Dataset B: Delete/Rebalance Stress

Use this on a fresh table if you want to focus on deletion and B+ tree rebalancing.

```sql
create table rebalance_t(id int, age int);

insert into rebalance_t (id, age) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 0);
insert into rebalance_t (id, age) values (11, 1), (12, 2), (13, 3), (14, 4), (15, 5), (16, 6), (17, 7), (18, 8), (19, 9), (20, 0);
insert into rebalance_t (id, age) values (21, 1), (22, 2), (23, 3), (24, 4), (25, 5), (26, 6), (27, 7), (28, 8), (29, 9), (30, 0);
insert into rebalance_t (id, age) values (31, 1), (32, 2), (33, 3), (34, 4), (35, 5), (36, 6), (37, 7), (38, 8), (39, 9), (40, 0);
insert into rebalance_t (id, age) values (41, 1), (42, 2), (43, 3), (44, 4), (45, 5), (46, 6), (47, 7), (48, 8), (49, 9), (50, 0);
insert into rebalance_t (id, age) values (51, 1), (52, 2), (53, 3), (54, 4), (55, 5), (56, 6), (57, 7), (58, 8), (59, 9), (60, 0);
insert into rebalance_t (id, age) values (61, 1), (62, 2), (63, 3), (64, 4), (65, 5), (66, 6), (67, 7), (68, 8), (69, 9), (70, 0);
insert into rebalance_t (id, age) values (71, 1), (72, 2), (73, 3), (74, 4), (75, 5), (76, 6), (77, 7), (78, 8), (79, 9), (80, 0);

create index idx_rebalance_id on rebalance_t(id);

delete from rebalance_t where rebalance_t.id = 1;
delete from rebalance_t where rebalance_t.id = 2;
delete from rebalance_t where rebalance_t.id = 3;
delete from rebalance_t where rebalance_t.id = 4;
delete from rebalance_t where rebalance_t.id = 5;
delete from rebalance_t where rebalance_t.id = 6;
delete from rebalance_t where rebalance_t.id = 7;
delete from rebalance_t where rebalance_t.id = 8;
delete from rebalance_t where rebalance_t.id = 9;
delete from rebalance_t where rebalance_t.id = 10;
delete from rebalance_t where rebalance_t.id = 11;
delete from rebalance_t where rebalance_t.id = 12;
delete from rebalance_t where rebalance_t.id = 13;
delete from rebalance_t where rebalance_t.id = 14;
delete from rebalance_t where rebalance_t.id = 15;
delete from rebalance_t where rebalance_t.id = 16;
delete from rebalance_t where rebalance_t.id = 17;
delete from rebalance_t where rebalance_t.id = 18;
delete from rebalance_t where rebalance_t.id = 19;
delete from rebalance_t where rebalance_t.id = 20;
delete from rebalance_t where rebalance_t.id = 21;
delete from rebalance_t where rebalance_t.id = 22;
delete from rebalance_t where rebalance_t.id = 23;
delete from rebalance_t where rebalance_t.id = 24;
delete from rebalance_t where rebalance_t.id = 25;
delete from rebalance_t where rebalance_t.id = 26;
delete from rebalance_t where rebalance_t.id = 27;
delete from rebalance_t where rebalance_t.id = 28;
delete from rebalance_t where rebalance_t.id = 29;
delete from rebalance_t where rebalance_t.id = 30;
delete from rebalance_t where rebalance_t.id = 31;
delete from rebalance_t where rebalance_t.id = 32;
delete from rebalance_t where rebalance_t.id = 33;
delete from rebalance_t where rebalance_t.id = 34;
delete from rebalance_t where rebalance_t.id = 35;
delete from rebalance_t where rebalance_t.id = 36;
delete from rebalance_t where rebalance_t.id = 37;
delete from rebalance_t where rebalance_t.id = 38;
delete from rebalance_t where rebalance_t.id = 39;
delete from rebalance_t where rebalance_t.id = 40;
delete from rebalance_t where rebalance_t.id = 41;
delete from rebalance_t where rebalance_t.id = 42;
delete from rebalance_t where rebalance_t.id = 43;
delete from rebalance_t where rebalance_t.id = 44;
delete from rebalance_t where rebalance_t.id = 45;
delete from rebalance_t where rebalance_t.id = 46;
delete from rebalance_t where rebalance_t.id = 47;
delete from rebalance_t where rebalance_t.id = 48;
delete from rebalance_t where rebalance_t.id = 49;
delete from rebalance_t where rebalance_t.id = 50;
delete from rebalance_t where rebalance_t.id = 51;
delete from rebalance_t where rebalance_t.id = 52;
delete from rebalance_t where rebalance_t.id = 53;
delete from rebalance_t where rebalance_t.id = 54;
delete from rebalance_t where rebalance_t.id = 55;
delete from rebalance_t where rebalance_t.id = 56;
delete from rebalance_t where rebalance_t.id = 57;
delete from rebalance_t where rebalance_t.id = 58;
delete from rebalance_t where rebalance_t.id = 59;
delete from rebalance_t where rebalance_t.id = 60;
delete from rebalance_t where rebalance_t.id = 61;
delete from rebalance_t where rebalance_t.id = 62;
delete from rebalance_t where rebalance_t.id = 63;
delete from rebalance_t where rebalance_t.id = 64;
delete from rebalance_t where rebalance_t.id = 65;

select * from rebalance_t where rebalance_t.id = 1;
select * from rebalance_t where rebalance_t.id = 66;
select * from rebalance_t where rebalance_t.id >= 78;
insert into rebalance_t (id, age) values (81, 1);
select * from rebalance_t where rebalance_t.id = 81;
```

Expected result:

```text
id = 1     -> empty
id = 66    -> one row
id >= 78   -> 78, 79, 80
id = 81    -> one row after insertion
```

Why this matters:

- Deleting 65 out of 80 indexed keys forces repeated B+ tree underflow handling.
- The test checks that borrow/merge/rebalance keeps later lookups correct.

## Optional Planner Check

Use `explain` or the existing `Task3IndexTest.hasIndexScan(...)` helper to verify plan selection.

Expected planner behavior:

```text
Before create index:
  select * from t where t.id = 42
  -> SeqScanOperator + FilterOperator

After create index idx_id on t(id):
  select * from t where t.id = 42
  -> IndexScanOperator

After drop index idx_id:
  select * from t where t.id = 42
  -> SeqScanOperator + FilterOperator
```

## Cleanup

```sql
drop index idx_age;
drop table t;

drop index idx_rebalance_id;
drop table rebalance_t;
```

If an index was already dropped earlier, skip the corresponding `drop index`.
