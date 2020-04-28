/*
 * test.c
 *
 *  Created on: Jan 9, 2020
 *      Author: jiafu
 */
#include <gurt/common.h>
#include <stdlib.h>
#include <string.h>

#include <libgen.h>
#include <stdio.h>
#include <daos.h>
#include <fcntl.h>
#include <daos_obj_class.h>
#include <sys/time.h>


struct map_info{
        daos_handle_t *map_objs;
        int     nbr;
};

const char* MAP_STR = "map";
const char* REDUCE_STR = "reduce";
const int MAP_OUTPUT_OBJ_HI = 0;
const int MAP_OUTPUT_OBJ_LOW = 1024;
int low_id = 0;
const daos_iod_type_t IOD_TYPE = DAOS_IOD_SINGLE;
const char *DKEY_TEMP = "dkey%d";
const char *AKEY_TEMP = "akey%d";
const daos_obj_id_t EMPTY_OID = {0};
const char *OBJ_HI_LO_TEMP = "%ld:%ld";

const int MAX_MAPS = 10485760;


void
dts_buf_render(char *buf, unsigned int buf_len)
{
        int     nr = 'z' - 'a' + 1;
        int     i;

        for (i = 0; i < buf_len - 1; i++) {
                int randv = rand() % (2 * nr);

                if (randv < nr)
                        buf[i] = 'a' + randv;
                else
                        buf[i] = 'A' + (randv - nr);
        }
        buf[i] = '\0';
}

int do_map(daos_handle_t coh, daos_handle_t oh, int key_id, int reduces, int part_size, char *update_buf) {
        int i, rc;
        int succeed = 1;

        d_iov_t                 val_iov;
        d_sg_list_t             sgl;
        daos_iod_t              iod;
        daos_key_t              dkey;

        sgl.sg_nr = 1;
        sgl.sg_iovs = &val_iov;
        iod.iod_recxs = NULL;
        iod.iod_nr = 1;
        iod.iod_type = IOD_TYPE;
        iod.iod_size = strlen(update_buf);

        d_iov_set(&val_iov, update_buf, strlen(update_buf));

        char *dkey_str = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
        char *akey_str = (char *)calloc(1, strlen(AKEY_TEMP) + 8);
        sprintf(akey_str, AKEY_TEMP, key_id);
        d_iov_set(&iod.iod_name, (void *)akey_str, strlen(akey_str));
        for (i = 0; i < reduces; i++) {
                sprintf(dkey_str, DKEY_TEMP, i);
                d_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));
                rc = daos_obj_update(oh, DAOS_TX_NONE, 0, &dkey, 1, &iod, &sgl, NULL);
                if (rc != 0) {
                        printf("failed to update object in map %d and reduce %d, rc: %d\n", key_id, i, rc);
                        succeed = 0;
                        goto out;
                }
        }

out:
        if (dkey_str) {
                free(dkey_str);
        }
        if (akey_str) {
                free(akey_str);
        }
        return !succeed;
}

daos_obj_id_t map(daos_handle_t coh, int maps, int reduces, int part_size, int idx, int nbr_of_map, int map_output_oid_low) {
        daos_obj_id_t output_id;
        daos_handle_t output_oh;
        struct timeval start, stop;

        output_id.hi = MAP_OUTPUT_OBJ_HI;
        output_id.lo = map_output_oid_low == -1 ? MAP_OUTPUT_OBJ_LOW : map_output_oid_low;
        daos_obj_generate_id(&output_id, 0, OC_SX, 0);
        int rc = daos_obj_open(coh, output_id, 0, &output_oh, NULL);
        if (rc != 0) {
                printf("failed to open root output object, rc: %d\n", rc);
                return EMPTY_OID;
        }

        printf("root map object opened\n");

//      rc = daos_obj_punch(output_oh, DAOS_TX_NONE, 0, NULL);
//      if (rc != 0) {
//              printf("failed to punch output object, rc: %d\n", rc);
//              goto out;
//      }

        int map_index = 0;
        char *update_buf = (char *)calloc(1, part_size);
        dts_buf_render(update_buf, part_size+1);
        printf("update buf size: %ld\n", strlen(update_buf));
        int i;
        daos_key_t dkey;

        printf("start to map\n");
        gettimeofday(&start, NULL);
        for (i = idx; i < (idx + nbr_of_map); i++) {
                rc = do_map(coh, output_oh, i, reduces, part_size, update_buf);
                if (rc != 0) {
                        printf("failed to run map %d, rc: %d\n", i, rc);
                        goto out;
                }
        }
        gettimeofday(&stop, NULL);
        uint64_t total_bytes = ((uint64_t)nbr_of_map)*part_size*reduces;
        uint64_t time = (stop.tv_sec - start.tv_sec)*1000 + (stop.tv_usec - start.tv_usec)/1000;
        printf("number of maps: %d\n", nbr_of_map);
        printf("map total types: %ld\n", total_bytes);
        printf("map took %ld milli-seconds\n", time);
        printf(":perf: %lf\n", (((double)total_bytes)/1024/1024)/(((double)time)/1000));

out:
        if (update_buf) {
                free(update_buf);
        }
        rc = daos_obj_close(output_oh, NULL);
        if (rc != 0) {
                printf("failed to close output object, rc: %d\n", rc);
                return EMPTY_OID;
        }
        return output_id;
}

void reduce(daos_handle_t coh, int maps, int reduces, int part_size, int idx, int nbr_of_reduce, int map_output_oid_low) {
		daos_obj_id_t output_id;
		daos_handle_t output_oh;

		output_id.hi = MAP_OUTPUT_OBJ_HI;
		output_id.lo = map_output_oid_low == -1 ? MAP_OUTPUT_OBJ_LOW : map_output_oid_low;
		daos_obj_generate_id(&output_id, 0, OC_SX, 0);
		int rc = daos_obj_open(coh, output_id, 0, &output_oh, NULL);
		if (rc != 0) {
				printf("failed to open root output object, rc: %d\n", rc);
				return ;
		}

        struct timeval start, stop;

        // for keys;
		int key_desc_nbr = maps;
		daos_key_desc_t *kds = (daos_key_desc_t *)calloc(key_desc_nbr, sizeof(daos_key_desc_t));
		daos_anchor_t    anchor;
		d_sg_list_t sgl;
		d_iov_t iov;
		char *bufs = (char *)calloc(key_desc_nbr, strlen(AKEY_TEMP) + 8);

		char **akeys = (char **)calloc(key_desc_nbr, sizeof(char *));

		uint32_t number;
		int akey_nbr = 0;
		int i, j, k;
		daos_key_t dkey;

		sgl.sg_nr = 1;
		sgl.sg_nr_out = 0;
		sgl.sg_iovs = &iov;
		d_iov_set(&iov, (void *)bufs, key_desc_nbr*(strlen(DKEY_TEMP) + 8));

		//for values
		d_iov_t                 *val_iov = (d_iov_t *)calloc(key_desc_nbr, sizeof(d_iov_t));
		d_sg_list_t             *val_sgl = (d_sg_list_t *)calloc(key_desc_nbr, sizeof(d_sg_list_t));
		daos_iod_t              *val_iod = (daos_iod_t *)calloc(key_desc_nbr, sizeof(daos_iod_t));
		char **val_bufs = (char **)calloc(key_desc_nbr, sizeof(char *));

		for (i = 0; i < key_desc_nbr; i++) {
				val_sgl[i].sg_nr = 1;
				val_sgl[i].sg_nr_out = 0;
				val_sgl[i].sg_iovs = &val_iov[i];
				val_bufs[i] = (char *)calloc(1, part_size);
				d_iov_set(&val_iov[i], (void *)val_bufs[i], part_size);
				val_iod[i].iod_nr = 1;
				val_iod[i].iod_type = IOD_TYPE;
		}

        char *dkey_str = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
        int count = 0;
        gettimeofday(&start, NULL);
        for (j = idx; j < (idx + nbr_of_reduce); j++) {
                sprintf(dkey_str, DKEY_TEMP, j);
                d_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));

                memset(&anchor, 0, sizeof(anchor));
				for (number = key_desc_nbr, akey_nbr = 0;
										 !daos_anchor_is_eof(&anchor);
										 number = key_desc_nbr) {

						rc = daos_obj_list_akey(output_oh, DAOS_TX_NONE, &dkey, &number, kds, &sgl, &anchor, NULL);
						if (rc != 0) {
								printf("failed to list akey, rc: %d\n", rc);
								goto out;
						}

						if (number == 0) {
								continue;
						}
//						printf("list akey: number is %d\n", number);
//						printf("list akey: akeys in buffer: %s\n", bufs);
						int index = 0;
						for (k = 0; k < number; k++) {
//								printf("length of akey kds %d is %ld\n", j, kds[j].kd_key_len);
								if (akey_nbr >= key_desc_nbr) {
									printf("number of akey %d should be no more than %d\n", akey_nbr, key_desc_nbr);
									goto out;
								}
								akeys[akey_nbr] = (char *)calloc(1, strlen(AKEY_TEMP) + 8);
								memcpy(akeys[akey_nbr], (void *)(bufs+index), kds[k].kd_key_len);
								akey_nbr++;
								index += kds[k].kd_key_len;
						}
				}

				if (akey_nbr != maps) {
					printf("number of akey %d should be equal to number of maps %d\n", akey_nbr, maps);
					goto out;
				}

                // get akey value
				for (k = 0; k < akey_nbr; k++) {
						d_iov_set(&val_iod[k].iod_name, (void *)akeys[k], strlen(akeys[k]));
//						printf("akeys: %s\n", akeys[j]);
				}
				rc = daos_obj_fetch(output_oh, DAOS_TX_NONE, 0, &dkey, akey_nbr, val_iod, val_sgl, NULL, NULL);
				if (rc != 0) {
						printf("failed to fetch akey values from dkey, %s, rc: %d\n", dkey_str, rc);
						goto out;
				}

				for (k = 0; k < akey_nbr; k++) {
					if (val_iod[k].iod_size != part_size) {
							printf("map output, %s : %s, content size, %ld, is not as expected, %d.\n", akeys[k], dkey_str, val_iod[k].iod_size, part_size);
							goto out;
					}
				}
        }
        gettimeofday(&stop, NULL);
        uint64_t total_bytes = ((uint64_t)nbr_of_reduce)*maps*part_size;
        uint64_t time = (stop.tv_sec - start.tv_sec)*1000 + (stop.tv_usec - start.tv_usec)/1000;
        printf("number of reduces: %d\n", nbr_of_reduce);
        printf("number of bytes: %ld\n", total_bytes);
        printf("reduce took %ld milli-seconds\n", time);
        printf(":perf: %lf\n", (((double)total_bytes)/1024/1024)/(((double)time)/1000));

out:
        if (dkey_str) {
                free(dkey_str);
        }
        if (kds) {
        	free(kds);
        }
        if (bufs) {
				free(bufs);
		}
		if (akeys) {
				for (i = 0; i < key_desc_nbr; i++) {
						if (akeys[i]) {
								free(akeys[i]);
						}
				}
				free(akeys);
		}
		if (val_iov) {
			free(val_iov);
		}
		if (val_sgl) {
			free(val_sgl);
		}
		if (val_iod) {
			free(val_iod);
		}
		if (val_bufs) {
				for (i = 0; i < key_desc_nbr; i++) {
						if (val_bufs[i]) {
								free(val_bufs[i]);
						}
				}
				free(val_bufs);
		}
		rc = daos_obj_close(output_oh, NULL);
		if (rc != 0) {
				printf("failed to close output object, rc: %d\n", rc);
		}
}

int is_null(char *str) {
        if (str == NULL || strlen(str) == 0) {
                return 1;
        }
        return 0;
}

int validate_part_total(int total, int start, int nbr, char *type) {
        if (total % nbr != 0) {
                printf("total %s(s) %d should be a multiple of number of %s %d\n", type, total, type, nbr);
                return 1;
        }
        if (nbr <= 0 || nbr > MAX_MAPS) {
                printf("number of %s %d should be between 1 and %d\n", type, nbr, MAX_MAPS);
                return 1;
        }
        if ((start + nbr) > total) {
                printf("start index %d + number of %s %d should be no more than %d\n", start, type, nbr, total);
                return 1;
        }
        return 0;
}

int validate_args(char *op, char *server_group, char *pool_str, char *cont_str, int maps, int reduces,
                int part_size, int idx, int nbr_of_mr, int map_output_oid_low) {
        if (is_null(server_group)) {
                printf("need server group\n");
                return 1;
        }
        if (is_null(pool_str)) {
                printf("need pool uuid\n");
                return 1;
        }
        if (is_null(cont_str)) {
                printf("need container uuid\n");
                return 1;
        }
        if (maps <= 0) {
                printf("total maps %d should be greater than 0\n", maps);
                return 1;
        }
        if (reduces <= 0) {
                printf("total reduces %d should be greater than 0\n", reduces);
                return 1;
        }
        if (part_size <= 0) {
                printf("reduce partition size %d should be greater than 0\n", part_size);
                return 1;
        }
        if (idx < 0 || idx >= maps) {
                printf("index %d should be between 0 and %d - 1\n", idx, maps);
                return 1;
        }
        if (!strcmp(MAP_STR, op)) {
                if (validate_part_total(maps, idx, nbr_of_mr, op)) {
                        return 1;
                }
                return 0;
        } else if (!strcmp(REDUCE_STR, op)) {
                if (validate_part_total(reduces, idx, nbr_of_mr, op)) {
                        return 1;
                }
                return 0;
        } else {
                printf("unknown operation, %s\n", op);
                return 1;
        }
}

int main(int argc, char *argv[])
{
        if (argc < 10) {
                printf("need arguments of map/reduce, server group, pool UUID,"
                    "container UUID, number of total maps, number of total reduces and partition size,"
                    "index, number of maps or reduces per process and map output object id \n");
                return 1;
        }
        char *op = argv[1]; //read or write
        char *server_group = argv[2];
        char *pool_str = argv[3];
        char *cont_str = argv[4];
        int maps = atoi(argv[5]);
        int reduces = atoi(argv[6]);
        int part_size = atoi(argv[7]);
        int idx = atoi(argv[8]);
        int nbr_of_mr = atoi(argv[9]);
        int map_output_oid_low = -1;
        if (argc > 10) {
                map_output_oid_low = atoi(argv[10]);
        }

        printf("op: %s\n", op);
        printf("server_group: %s\n", server_group);
        printf("pool_str: %s\n", pool_str);
        printf("cont_str: %s\n", cont_str);
        printf("maps: %d\n", maps);
        printf("reduces: %d\n", reduces);
        printf("part_size: %d\n", part_size);
        printf("idx: %d\n", idx);
        printf("nbr_of_mr: %d\n", nbr_of_mr);
        printf("map_output_oid_low: %d\n", map_output_oid_low);

        validate_args(op, server_group, pool_str, cont_str, maps, reduces, part_size, idx, nbr_of_mr, map_output_oid_low);

        int rc = daos_init();

        if (rc) {
                printf("daos_init() failed with rc = %d\n", rc);
                return rc;
        }
        printf("daos inited\n");
        /* connect to pool */
        uuid_t pool_uuid;

        uuid_parse(pool_str, pool_uuid);
        d_rank_list_t *svcl = daos_rank_list_parse("0", ":");
        daos_handle_t poh = {0};

        rc = daos_pool_connect(pool_uuid, server_group, svcl,
                                        2, /* read write */
                                        &poh /* returned pool handle */,
                                        NULL /* returned pool info */,
                                        NULL /* event */);

        if (rc) {
                printf("Failed to connect to pool (%s), rc is %d\n",
                    pool_str, rc);
                return rc;
        }
        printf("pool connected\n");
        /* connect to container */
        uuid_t cont_uuid;

        uuid_parse(cont_str, cont_uuid);
        daos_cont_info_t co_info;
        daos_handle_t coh = {0};

        rc = daos_cont_open(poh, cont_uuid, 2, &coh, &co_info, NULL);
        if (rc) {
                printf("Failed to connect to container (%s)\n", cont_str);
                goto quit;
        }

        printf("container opened\n");
        if (!strcmp(MAP_STR, op)) {
                map(coh, maps, reduces, part_size, idx, nbr_of_mr, map_output_oid_low);
        } else if (!strcmp(REDUCE_STR, op)) {
                reduce(coh, maps, reduces, part_size, idx, nbr_of_mr, map_output_oid_low);
        } else {
                printf("unknown operation, %s\n", op);
        }
quit:
        if (poh.cookie != 0) {
                daos_pool_disconnect(poh, NULL);
        }
        if (coh.cookie != 0) {
                daos_cont_close(coh, NULL);
        }

        daos_fini();
}

