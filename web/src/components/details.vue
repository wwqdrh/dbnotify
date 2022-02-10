<template>
  <div>
    <el-dialog
      title="详情"
      :visible.sync="dialogVisible"
      width="50%"
      :before-close="handleClose"
      :close-on-click-modal="false"
    >
      <div class="body" style="height: calc(100vh - 370px); overflow: auto">
        <el-card
          class="box-card"
          v-for="(item, index) in infoContent"
          :key="index"
          style="margin-bottom: 10px"
        >
          <div class="time" style="margin-bottom: 10px">
            <span style="margin-right: 10px">
              <span class="label_name">时间：</span
              >{{ getLocalTime(item.time) }}</span
            >
            <span v-if="item.action == 'update'" style="color: #e6a23c"
              >更新</span
            >
            <span v-else-if="item.action == 'insert'" style="color: #67c23a"
              >创建</span
            >
            <span v-else-if="item.action == 'delete'" style="color: #f56c6c"
              >删除</span
            >

            <span
              style="float: right; color: #409eff; cursor: pointer"
              @click="AllShow(index)"
              v-if="itemShow == index && kai"
              >关闭</span
            >
            <span
              style="float: right; color: #409eff; cursor: pointer"
              @click="AllShow(index)"
              v-else
              >展开</span
            >
          </div>

          <div class="action" v-if="item.action == 'ddl'">
            <!-- {{ item.data}} -->
            <span style="margin-right: 10px" class="label_name">{{
              item.data.sql
            }}</span>
          </div>
          <div class="action" v-if="item.action == 'update'">
            <!-- {{ item.data}} -->
            <div
              v-for="(i, index) in item.data"
              :key="index"
              class="action_item"
            >
              <div>
                <span style="margin-right: 10px" class="label_name"
                  >{{ index }}:</span
                >
                <span style="margin-right: 20px">更新前：{{ i.before }} </span>
                <span> 更新后：{{ i.after }}</span>
              </div>
            </div>
          </div>
          <div class="action" v-if="item.action == 'insert'">
            <!-- {{ item.data}} -->
            <div
              v-for="(i, index) in item.data"
              :key="index"
              class="action_item"
            >
              <div>
                <span style="margin-right: 10px" class="label_name"
                  >{{ index }}:</span
                >{{ i }}
              </div>
            </div>
          </div>
          <div class="action" v-if="item.action == 'delete'">
            <!-- {{ item.data}} -->
            <div
              v-for="(i, index) in item.data"
              :key="index"
              class="action_item"
            >
              <div>
                <span style="margin-right: 10px" class="label_name"
                  >{{ index }}:</span
                >{{ i }}
              </div>
            </div>
          </div>

          <el-card
            class="box-card"
            v-for="(citem, cindex) in item.relations"
            :key="cindex"
            style="margin-bottom: 10px"
          >
            <div class="time" style="margin-bottom: 10px">
              <span style="margin-right: 10px">
                <span class="label_name">时间：</span
                >{{ getLocalTime(citem.time) }}</span
              >
              <span v-if="citem.action == 'update'" style="color: #e6a23c"
                >更新</span
              >
              <span v-else-if="citem.action == 'insert'" style="color: #67c23a"
                >创建</span
              >
              <span v-else-if="citem.action == 'delete'" style="color: #f56c6c"
                >删除</span
              >
            </div>

            <div class="action" v-if="item.action == 'update'">
              <!-- {{ item.data}} -->
              <div
                v-for="(i, index) in item.data"
                :key="index"
                class="action_item"
              >
                <div>
                  <span style="margin-right: 10px" class="label_name"
                    >{{ index }}:</span
                  >
                  <span style="margin-right: 20px"
                    >更新前：{{ i.before }}
                  </span>
                  <span> 更新后：{{ i.after }}</span>
                </div>
              </div>
            </div>
            <div class="action" v-if="item.action == 'insert'">
              <!-- {{ item.data}} -->
              <div
                v-for="(i, index) in item.data"
                :key="index"
                class="action_item"
              >
                <div>
                  <span style="margin-right: 10px" class="label_name"
                    >{{ index }}:</span
                  >{{ i }}
                </div>
              </div>
            </div>
            <div class="action" v-if="item.action == 'delete'">
              <!-- {{ item.data}} -->
              <div
                v-for="(i, index) in item.data"
                :key="index"
                class="action_item"
              >
                <div>
                  <span style="margin-right: 10px" class="label_name"
                    >{{ index }}:</span
                  >{{ i }}
                </div>
              </div>
            </div>

            <div v-show="itemShow == index && kai" class="info_content">
              <div class="border"></div>
              <div v-for="(i, index) in item.all" :key="index">
                <div class="item_style">
                  <span style="margin-right: 10px; color: rgb(111, 126, 126)"
                    >{{ index }}:</span
                  >{{ i }}
                </div>
              </div>
            </div>
          </el-card>

          <div v-show="itemShow == index && kai" class="info_content">
            <div class="border"></div>
            <div v-for="(i, index) in item.all" :key="index">
              <div class="item_style">
                <span style="margin-right: 10px; color: rgb(111, 126, 126)"
                  >{{ index }}:</span
                >{{ i }}
              </div>
            </div>
          </div>
        </el-card>
      </div>
      <div class="fenye">
        <el-pagination
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
          :current-page="currentPage"
          :page-sizes="[10, 20, 50, 100]"
          :page-size="10"
          layout="total, sizes, prev, pager, next, jumper"
          :total="total"
        >
        </el-pagination>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import axios from "axios";
export default {
  data() {
    return {
      dialogVisible: false,
      infoContent: [],
      itemShow: -1,
      kai: false,
      currentPage: 1,
      page: 1,
      page_size: 10,
      total: 0,
      record_id: null,
    };
  },
  props: {
    tableHiostory: Array,
    indexData: Object,
    tableName: String,
  },
  created() {
    this.index = this.rowIndex;
  },
  watch: {},
  methods: {
    isShow() {
      this.dialogVisible = true;
      const idStr = Object.keys(this.indexData).join();
      const valueStr = Object.values(this.indexData).join();
      const label = idStr + "=" + valueStr;
      this.record_id = label;
      this.postHistory(
        this.tableName,
        this.record_id,
        this.page,
        this.page_size
      );
    },
    handleClose() {
      this.dialogVisible = false;
      this.itemShow = -100;
    },
    getLocalTime(nS) {
      const a = new Date(nS).getTime();
      return new Date(parseInt(a)).toLocaleString().replace(/:\d{1,2}$/, " ");
    },
    AllShow(idnex) {
      console.log(idnex);
      this.itemShow = idnex;
      this.kai = !this.kai;
    },
    handleSizeChange(val) {
      console.log(`每页 ${val} 条`);
      this.page_size = val;
      this.postHistory(
        this.tableName,
        this.record_id,
        this.page,
        this.page_size
      );
    },
    handleCurrentChange(val) {
      console.log(`当前页: ${val}`);
      this.page = val;
      this.postHistory(
        this.tableName,
        this.record_id,
        this.page,
        this.page_size
      );
    },
    postHistory(name, id, page, size) {
      const data = {
        table_name: name,
        record_id: id,
        page: page,
        page_size: size,
      };
      const headers = {
        "Content-Type": "application/json",
      };
      axios
        .post("/bdatalog/list_history_by_name", data, { headers: headers })
        .then((res) => {
          // console.log(res);
          if (res.data.no == 0) {
            this.total = res.data.data[0].log.length;
            const newHiostoryArr = [];
            for (let i = 0; i < this.total; i++) {
              newHiostoryArr.push(res.data.data[0].log[i]);
            }
            this.infoContent = newHiostoryArr;
          } else {
            this.$message.error(res.data.data);
            console.log(res.data);
          }
        })
        .catch((res) => {
          this.$message.error(res);
        });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .el-dialog {
  height: calc(100vh - 200px);
  overflow: auto;
}
.body {
  .time {
    .label_name {
      color: rgb(111, 126, 126);
    }
  }
  .action {
    .action_item {
      border-top: 1px dashed #eee;
      border-bottom: 1px dashed #eee;
      padding: 5px 0px;
    }
    .label_name {
      color: rgb(111, 126, 126);
    }
  }

  .info_content {
    .border {
      border-top: 1px solid rgb(109, 108, 108);
      margin: 20px 0px;
    }
    .item_style {
      margin: 5px 0px;
    }
  }
}
.fenye {
  position: absolute;
  bottom: 20px;
}
</style>
