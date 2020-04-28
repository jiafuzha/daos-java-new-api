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

const int MAX_MAPS_PER_PROCESS = 512;


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

daos_obj_id_t do_map(daos_handle_t coh, int key_id, int reduces, int part_size, char *update_buf) {
        daos_obj_id_t map_id;
        daos_handle_t oh;
        map_id.hi = (1000000 + key_id);
        map_id.lo = key_id;
        daos_obj_generate_id(&map_id, 0, OC_SX, 0);
        int rc = daos_obj_open(coh, map_id, 0, &oh, NULL);
        if (rc != 0) {
                printf("failed to open output object in map %d, rc: %d\n", key_id, rc);
                return EMPTY_OID;
        }

        int i;
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
        for (i = 0; i < reduces; i++) {
                sprintf(dkey_str, DKEY_TEMP, i);
                sprintf(akey_str, AKEY_TEMP, i);
                d_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));
                d_iov_set(&iod.iod_name, (void *)akey_str, strlen(akey_str));
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
        rc = daos_obj_close(oh, NULL);
        if (rc != 0) {
                printf("failed to close map output object in map %d, rc: %d\n", key_id, rc);
                return EMPTY_OID;
        }
        if (succeed) {
                return map_id;
        }
        return EMPTY_OID;
}

daos_obj_id_t map(daos_handle_t coh, int maps, int reduces, int part_size, int idx, int nbr_of_map, int map_output_oid_low) {
        daos_obj_id_t output_id;
        daos_handle_t output_oh;
        daos_obj_id_t map_ids[nbr_of_map];
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
        char *dkey_str = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
        char **akeys = (char **)calloc(nbr_of_map, sizeof(char *));
        d_iov_t                 *val_iov = (d_iov_t *)calloc(nbr_of_map, sizeof(d_iov_t));
        d_sg_list_t             *sgl = (d_sg_list_t *)calloc(nbr_of_map, sizeof(d_sg_list_t));
        daos_iod_t              *iod = (daos_iod_t *)calloc(nbr_of_map, sizeof(daos_iod_t));
        char **oid_buffs = (char **)calloc(nbr_of_map, sizeof(char *));

        printf("start to map\n");
        int count = 0;
        gettimeofday(&start, NULL);
        for (i = idx; i < (idx + nbr_of_map); i++) {
                daos_obj_id_t map_id = do_map(coh, i, reduces, part_size, update_buf);
                if (map_id.hi == 0) {
                        printf("failed to run map %d, rc: %d\n", i, rc);
                        goto out;
                }
                map_ids[map_index++] = map_id;
                count++;
        }
        gettimeofday(&stop, NULL);
        uint64_t total_bytes = ((uint64_t)nbr_of_map)*part_size*reduces;
        uint64_t time = (stop.tv_sec - start.tv_sec)*1000 + (stop.tv_usec - start.tv_usec)/1000;
        printf("number of maps: %d\n", count);
        printf("map total types: %ld\n", total_bytes);
        printf("map took %ld milli-seconds\n", time);
        printf(":perf: %lf\n", (((double)total_bytes)/1024/1024)/(((double)time)/1000));

        sprintf(dkey_str, DKEY_TEMP, idx);
        for (i = idx, map_index = 0; i < (idx + nbr_of_map); i++, map_index++) {
                akeys[map_index] = (char *)calloc(1, strlen(AKEY_TEMP) + 8);
                sprintf(akeys[map_index], AKEY_TEMP, i);
        }
        printf("akeys set\n");

        for (i = 0; i < nbr_of_map; i++) {
                sgl[i].sg_nr = 1;
                sgl[i].sg_iovs = &val_iov[i];

                oid_buffs[i] = (char *)calloc(1, strlen(OBJ_HI_LO_TEMP) + 32);
                sprintf(oid_buffs[i], OBJ_HI_LO_TEMP, map_ids[i].hi, map_ids[i].lo);
                d_iov_set(&iod[i].iod_name, (void *)akeys[i], strlen(akeys[i]));
                d_iov_set(&val_iov[i], (void *)oid_buffs[i], strlen(oid_buffs[i]));

                iod[i].iod_recxs = NULL;
                iod[i].iod_nr = 1;
                iod[i].iod_type = IOD_TYPE;
                iod[i].iod_size = strlen(oid_buffs[i]);
        }
        printf("sgl & iod set\n");
        d_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));
        rc = daos_obj_update(output_oh, DAOS_TX_NONE, 0, &dkey, nbr_of_map, iod, sgl, NULL);
        if (rc != 0) {
                printf("failed to save all map objects to root output object %ld:%ld, rc: %d\n", output_id.hi, output_id.lo, rc);
        }
        printf("map root object updated\n");

out:
        if (dkey_str) {
                free(dkey_str);
        }
        if (akeys) {
                for (i = 0; i < nbr_of_map; i++) {
                        if (akeys[i]) {
                                free(akeys[i]);
                        }
                }
                free(akeys);
        }
        if (update_buf) {
                free(update_buf);
        }
        if (val_iov) {
                free(val_iov);
        }
        if (sgl) {
                free(sgl);
        }
        if (iod) {
                free(iod);
        }
        if (oid_buffs) {
                for (i = 0; i < nbr_of_map; i++) {
                        if (oid_buffs[i]) {
                                free(oid_buffs[i]);
                        }
                }
                free(oid_buffs);
        }
        rc = daos_obj_close(output_oh, NULL);
        if (rc != 0) {
                printf("failed to close output object, rc: %d\n", rc);
                return EMPTY_OID;
        }
        return output_id;
}

struct map_info get_map_objs(daos_handle_t coh, int maps, int reduces, int map_output_oid_low) {
        daos_obj_id_t output_id;
        daos_handle_t output_oh;
        struct timeval start, stop;
        struct map_info info;
        info.map_objs = NULL;
        info.nbr = 0;

        output_id.hi = MAP_OUTPUT_OBJ_HI;
        output_id.lo = map_output_oid_low == -1 ? MAP_OUTPUT_OBJ_LOW : map_output_oid_low;
        daos_obj_generate_id(&output_id, 0, OC_SX, 0);
        int rc = daos_obj_open(coh, output_id, 0, &output_oh, NULL);
        if (rc != 0) {
                printf("failed to open root output object, rc: %d\n", rc);

                return info;
        }

        // for keys;
        int key_desc_nbr = 512;
        daos_key_desc_t kds[key_desc_nbr];
        daos_anchor_t    anchor;
//      d_sg_list_t sgl[key_desc_nbr];
        d_sg_list_t sgl;
//      d_iov_t iov[key_desc_nbr];
        d_iov_t iov;
        char **keys = (char **)calloc(key_desc_nbr, sizeof(char *));
        int key_array_len = key_desc_nbr;
//      char **bufs = (char **)calloc(key_desc_nbr, sizeof(char *));
        char *bufs = (char *)calloc(key_desc_nbr, strlen(DKEY_TEMP) + 8);

        char **akeys = (char **)calloc(key_desc_nbr, sizeof(char *));
        int akey_array_len = key_desc_nbr;

        uint32_t number;
        int key_nbr = 0;
        int i;
        daos_key_t dkey;

        //for values
        d_iov_t                 val_iov[MAX_MAPS_PER_PROCESS];
        d_sg_list_t             val_sgl[MAX_MAPS_PER_PROCESS];
        daos_iod_t              val_iod[MAX_MAPS_PER_PROCESS];
        char **val_bufs = (char **)calloc(MAX_MAPS_PER_PROCESS, sizeof(char *));
        daos_obj_id_t map_ids[maps];

        daos_handle_t *map_objs = (daos_handle_t *)calloc(maps, sizeof(daos_handle_t));
        int succeed = 0;

        memset(&anchor, 0, sizeof(anchor));

//      for (i = 0; i < key_desc_nbr; i++) {
//              sgl[i].sg_nr = 1;
//              sgl[i].sg_nr_out = 0;
//              sgl[i].sg_iovs = &iov[i];
//              bufs[i] = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
//              d_iov_set(&iov[i], (void *)bufs[i], strlen(DKEY_TEMP) + 8);
//      }

        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &iov;
        d_iov_set(&iov, (void *)bufs, key_desc_nbr*(strlen(DKEY_TEMP) + 8));

        for (i = 0; i < MAX_MAPS_PER_PROCESS; i++) {
                val_sgl[i].sg_nr = 1;
                val_sgl[i].sg_nr_out = 0;
                val_sgl[i].sg_iovs = &val_iov[i];
                val_bufs[i] = (char *)calloc(1, 40);
                d_iov_set(&val_iov[i], (void *)val_bufs[i], 40);
                val_iod[i].iod_nr = 1;
//              val_iod[i].iod_size = 40;
                val_iod[i].iod_type = IOD_TYPE;
        }

        for (number = key_desc_nbr, key_nbr = 0;
                         !daos_anchor_is_eof(&anchor);
                         number = key_desc_nbr) {

                rc = daos_obj_list_dkey(output_oh, DAOS_TX_NONE, &number, kds, &sgl, &anchor,
                                                NULL);

                if (rc != 0) {
                        printf("failed to list dkey, rc: %d\n", rc);
                        goto out;
                }

                if (number == 0) {
                        continue;
                }

                printf("list dkey: number is %d\n", number);
                printf("list dkey: dkeys in buffer: %s\n", bufs);

                int index = 0;
                for (i = 0; i < number; i++) {
                        printf("length of akey kds %d is %ld\n", i, kds[i].kd_key_len);
                        if (key_nbr >= key_array_len) {
                                key_array_len += key_desc_nbr;
                                keys = (char **)realloc(keys, (sizeof(char *))*key_array_len);
                        }
                        keys[key_nbr] = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
                        memcpy(keys[key_nbr], (void *)(bufs+index), kds[i].kd_key_len);
                        key_nbr++;
                        index += kds[i].kd_key_len;
                }
        }

        printf("total dkeys: %d\n", key_nbr);

        // list akeys
//      memset(&anchor, 0, sizeof(anchor));
        int akey_nbr = 0;
        int total_akeys = 0;
        int map_id_index = 0;
        char *deli = ":";
        int j;
        for (i = 0; i < key_nbr; i++) {
                printf("listing akyes of dkey %s\n", keys[i]);
                d_iov_set(&dkey, (void *)keys[i], strlen(keys[i]));
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
                        printf("list akey: number is %d\n", number);
                        printf("list akey: akeys in buffer: %s\n", bufs);
                        int index = 0;
                        for (j = 0; j < number; j++) {
                                printf("length of akey kds %d is %ld\n", j, kds[j].kd_key_len);
                                if (akey_nbr >= akey_array_len) {
                                        akey_array_len += key_desc_nbr;
                                        akeys = (char **)realloc(akeys, (sizeof(char *))*akey_array_len);
                                }
                                akeys[akey_nbr] = (char *)calloc(1, strlen(AKEY_TEMP) + 8);
                                memcpy(akeys[akey_nbr], (void *)(bufs+index), kds[j].kd_key_len);
                                akey_nbr++;
                                index += kds[j].kd_key_len;
                        }
                }

                if (akey_nbr > MAX_MAPS_PER_PROCESS) {
                        printf("number of akey, %d, should be no greater than %d\n", akey_nbr, MAX_MAPS_PER_PROCESS);
                        goto out;
                }

                total_akeys += akey_nbr;
                if (total_akeys > maps) {
                        printf(" %d, is not equal to one stored in map output root object, %d\n", maps, akey_nbr);
                        goto out;
                }
                printf("got %d akyes of dkey %s\n", akey_nbr, keys[i]);
                // get akey value
                for (j = 0; j < akey_nbr; j++) {
                        d_iov_set(&val_iod[j].iod_name, (void *)akeys[j], strlen(akeys[j]));
                        printf("akeys: %s\n", akeys[j]);
                }
                rc = daos_obj_fetch(output_oh, DAOS_TX_NONE, 0, &dkey, akey_nbr, val_iod, val_sgl, NULL, NULL);
                if (rc != 0) {
                        printf("failed to fetch akey values from dkey, %s, rc: %d\n", keys[i], rc);
                        goto out;
                }

                for (j = 0; j < akey_nbr; j++) {
                        printf("ids %d: %s\n", j, val_bufs[j]);
                        char *str = strdup(val_bufs[j]);
                        char *p = strtok(str, deli);
                        if (p == NULL) {
                                printf("bad obj str: %s\n", val_bufs[j]);
                                free(str);
                                goto out;
                        }
                        map_ids[map_id_index].hi = strtol(p, NULL, 10);
                        p = strtok(NULL, deli);
                        if (p == NULL) {
                                printf("bad obj low str: %s\n", val_bufs[j]);
                                free(str);
                                goto out;
                        }
                        map_ids[map_id_index].lo = strtol(p, NULL, 10);
                        map_id_index++;
                        free(str);
                }

        }

//      if (total_akeys < maps) {
//              printf("number of map objects, %d retrieved from map root object is less than given maps, %d \n", total_akeys, maps);
//              goto out;
//      }

        for (i = 0; i < total_akeys; i++) {
                rc = daos_obj_open(coh, map_ids[i], 0, &map_objs[i], NULL);
                if (rc != 0) {
                        printf("failed to open map object %ld : %ld, rc: %d\n", map_ids[i].hi, map_ids[i].lo, rc);
                        goto out;
                }
        }
        succeed = 1;

out:
        if (keys) {
                for (i = 0; i < key_array_len; i++) {
                        if (keys[i]) {
                                free(keys[i]);
                        }
                }
                free(keys);
        }
        if (bufs) {
//              for (i = 0; i < key_desc_nbr; i++) {
//                      if (bufs[i]) {
//                              free(bufs[i]);
//                      }
//              }
                free(bufs);
        }
        if (akeys) {
                for (i = 0; i < akey_array_len; i++) {
                        if (akeys[i]) {
                                free(akeys[i]);
                        }
                }
                free(akeys);
        }
        if (val_bufs) {
                for (i = 0; i < MAX_MAPS_PER_PROCESS; i++) {
                        if (val_bufs[i]) {
                                free(val_bufs[i]);
                        }
                }
                free(val_bufs);
        }
        rc = daos_obj_close(output_oh, NULL);
        if (rc != 0) {
                printf("failed to close output object, rc: %d\n", rc);
                succeed = 0;
        }
        if (succeed) {
                info.map_objs = map_objs;
                info.nbr = total_akeys;
                return info;
        }
        free(map_objs);
        return info;
}

void reduce(daos_handle_t coh, int maps, int reduces, int part_size, int idx, int nbr_of_reduce, int map_output_oid_low) {
        struct map_info info = get_map_objs(coh, maps, reduces, map_output_oid_low);
        daos_handle_t * map_objs = info.map_objs;
        int map_nbr = info.nbr;
        printf("total maps: %d\n", map_nbr);
        if (!map_objs) {
                printf("failed to get object ids from map.\n");
                return;
        }
        int i, j, rc;
        struct timeval start, stop;
        d_iov_t                 val_iov;
        d_sg_list_t             sgl;
        daos_iod_t              iod;
        daos_key_t              dkey;
        char *fetch_buf = (char *)calloc(1, part_size+1);

        sgl.sg_nr = 1;
        sgl.sg_iovs = &val_iov;
//      sgl.sg_nr_out = 0;
        iod.iod_recxs = NULL;
        iod.iod_nr = 1;
        iod.iod_type = IOD_TYPE;
        iod.iod_size = part_size;

        d_iov_set(&val_iov, fetch_buf, part_size);

        char *dkey_str = (char *)calloc(1, strlen(DKEY_TEMP) + 8);
        char *akey_str = (char *)calloc(1, strlen(AKEY_TEMP) + 8);
        int count = 0;
        gettimeofday(&start, NULL);
        for (j = idx; j < (idx + nbr_of_reduce); j++) {
                sprintf(dkey_str, DKEY_TEMP, j);
                sprintf(akey_str, AKEY_TEMP, j);
                d_iov_set(&dkey, (void *)dkey_str, strlen(dkey_str));
                d_iov_set(&iod.iod_name, (void *)akey_str, strlen(akey_str));
                for (i = 0; i < map_nbr; i++) {
                        rc = daos_obj_fetch(map_objs[i], DAOS_TX_NONE, 0, &dkey, 1, &iod, &sgl, NULL, NULL);
                        if (rc != 0) {
                                printf("failed to fetch reduce (%d) content from map object %d, rc: %d\n", j, i, rc);
                                goto out;
                        }
                        if (iod.iod_size != part_size) {
                                printf("reduce, %s : %s, content size from map %d, %ld, is not expected, %d.\n", dkey_str, akey_str, i, iod.iod_size, part_size);
                                continue;
                        }
//                      printf("debug: reduce size: %ld\n", iod.iod_size);
                }
                count++;
        }
        gettimeofday(&stop, NULL);
        uint64_t total_bytes = ((uint64_t)nbr_of_reduce)*map_nbr*part_size;
        uint64_t time = (stop.tv_sec - start.tv_sec)*1000 + (stop.tv_usec - start.tv_usec)/1000;
        printf("number of reduces: %d\n", count);
        printf("number of bytes: %ld\n", total_bytes);
        printf("reduce took %ld milli-seconds\n", time);
        printf(":perf: %lf\n", (((double)total_bytes)/1024/1024)/(((double)time)/1000));

out:
        if (map_objs) {
                for (i = 0; i < maps; i++) {
                        if (map_objs[i].cookie != 0) {
                                daos_obj_close(map_objs[i], NULL);
                        }
                }
                free(map_objs);
        }
        if (fetch_buf) {
                free(fetch_buf);
        }
        if (dkey_str) {
                free(dkey_str);
        }
        if (akey_str) {
                free(akey_str);
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
        if (nbr <= 0 || nbr > MAX_MAPS_PER_PROCESS) {
                printf("number of %s %d should be between 1 and %d\n", type, nbr, MAX_MAPS_PER_PROCESS);
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

