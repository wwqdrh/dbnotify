<template>
  <div class="Tab_Content">
    <div class="content_header" style="margin-bottom: 20px">
      <span>筛选表：</span>
      <el-select
        v-model="value"
        filterable
        placeholder="请选择"
        @change="selectChanged"
        style="margin-right: 20px"
      >
        <el-option
          v-for="item in options"
          :key="item.table_id"
          :label="item.table_name"
          :value="item.table_name"
        >
          <span :class="[!item.is_listen ? 'bgc allAction' : 'fifty-yuan allAction',]"  style="display:inline-block; width:95%">{{
            item.table_name
          }}</span>
          <i
            v-if="item.is_listen"
            title="取消监听"
            class="el-icon-delete"
            style="float: right; line-height: inherit"
            @click.stop="notListen(item)"
          ></i>
          <i
            v-else
            title="添加监听"
            class="el-icon-edit"
            style="float: right; line-height: inherit"
            @click.stop="editListen(item)"
          ></i>
        </el-option>
      </el-select>
      <el-date-picker
        style="margin-right: 20px"
        v-model="value1"
        type="daterange"
        format="yyyy 年 MM 月 dd 日"
        value-format="timestamp"
        range-separator="至"
        start-placeholder="开始日期"
        end-placeholder="结束日期"
      >
      </el-date-picker>
      <el-button type="primary" @click="screen">筛选</el-button>
      <el-button type="primary" @click="edit">编辑表</el-button>
    </div>
    <div class="content_table">
      <ag-grid-vue
        rowSelection="multiple"
        style="width: 100%"
        :resizable="true"
        class="table ag-theme-balham"
        :columnDefs="columnDefs"
        :rowData="rowData"
        :enableColResize="true"
        @gridReady="onGridReady"
        @cellClicked="onCellClicked"
        :defaultColDef="defaultColDef"
      ></ag-grid-vue>
    </div>
    <div class="content_bottom">
      <el-pagination
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :current-page="currentPage"
        :page-sizes="[20, 50, 100, 200]"
        :page-size="20"
        layout="total, sizes, prev, pager, next, jumper"
        :total="total"
      >
      </el-pagination>
    </div>
    <!-- 详情弹框 -->
    <Details
      ref="details"
      :tableHiostory="tableHiostory"
      :rowIndex="rowIndex"
      :indexData="indexData"
      :tableName="value"
    ></Details>
    <!-- 修改表 -->
    <EditTable
      ref="editTable"
      :tableName="value"
      :AllTable="editOptions"
      :actionName="actionName"
    ></EditTable>
  </div>
</template>
 
<script>
// 引入ag-grid-vue
import { AgGridVue } from "ag-grid-vue";
import Details from "./components/details";
import EditTable from "./components/editTable.vue";
import axios from "axios";
export default {
  components: { AgGridVue, Details, EditTable },
  data() {
    return {
      options: [],
      value: "", // 表名
      value1: [], // 时间戳
      columnDefs: [],
      rowData: [],
      gridApi: null,
      columnApi: null,
      currentPage: 1,
      total: 0,
      page: 1,
      page_size: 10,
      tableHiostory: [],
      rowIndex: null,
      indexData: {
        id: "",
        uid: "",
      },
      editOptions: [],
      defaultColDef: null,
      gridparams: null,
      actionName:''
    };
  },
  created() {
    this.getListName(); // 获取所有的表
  },

  beforeMount() {
    this.defaultColDef = {
      resizable: true, // 允许调整列的大小
      filter: true, // 使用默认过滤器
    };
  },

  methods: {
    onGridReady(params) {
      // console.log(params);
      // 获取gridApi
      this.gridApi = params.api;
      this.columnApi = params.columnApi;
      // 这时就可以通过gridApi调用ag-grid的传统方法了
      this.gridApi.sizeColumnsToFit();
    },
    sizeColumns() {
      this.gridApi.sizeColumnsToFit();
    },
    getListName() {
      axios
        .get("/bdatalog/list_table")
        .then((res) => {
          this.options = res.data.data;
          this.value = this.options[0].table_name;
          if(this.options[0].is_listen){
            this.getTableHistory();
          }
          
        })
        .catch((err) => {
          this.$message.error(err);
          console.log(err);
        });
    },

    selectChanged() {
      this.getTableHistory();
    },
    // 获取表
    getTableHistory() {
      const data = {
        table_name: this.value,
        start_time: this.value1 ? this.value1[0] : null,
        end_time: this.value1 ? this.value1[1] : null,
        page: this.page,
        page_size: this.page_size,
      };
      const headers = {
        "Content-Type": "application/json",
      };
      axios
        .post("/bdatalog/list_history_all", data, { headers: headers })
        .then((res) => {
          if (res.data.no == 0) {
            this.tableHiostory = res.data.data;
            if (this.tableHiostory.length != 0) {
              let NewPrimary = res.data.data[0].primary.split("=");
              let KeysArr = NewPrimary[0].split(",");
              let resultKey = [];
              for (var key in KeysArr) {
                let keyobj = { headerName: "", field: "" };
                keyobj.field = KeysArr[key];
                keyobj.headerName = KeysArr[key];

                resultKey.push(keyobj);
              }
              this.columnDefs = resultKey;
              this.columnDefs.push({
                headerName: "操作",
                width: 80,
                field: "operation",
                pinned: "right",
                suppressSizeToFit: true,
                cellStyle: {
                  color: "#409eff",
                  cursor: "pointer",
                  display: "flex",
                  justifyContent: "center",
                  alignItems: "center",
                },
                valueGetter: function () {
                  return "详情";
                },
                cellRenderer: function () {
                  return "<span>" + "详情" + "</span>";
                },
              });
              let valueArr = [];
              for (let j = 0; j < res.data.data.length; j++) {
                // console.log(res.data.data[j].primary.split("="));
                let valuesArr = res.data.data[j].primary
                  .split("=")[1]
                  .split(",");
                const obj = {};
                for (let i = 0; i < KeysArr.length; i++) {
                  obj[KeysArr[i]] = valuesArr[i];
                }
                valueArr.push(obj);
              }
              this.rowData = valueArr;
              this.total = res.data.data.length;
            } else {
              this.rowData = [];
              this.columnDefs = [];
              this.total = 0;
            }
          } else {
            this.$message.error(res.data.data);
            console.log(res.data.data);
          }
        })
        .catch((err) => {
          this.$message.error(err);
          console.log(err);
        })
        .finally(() => {
          this.sizeColumns();
        });
    },
    // 筛选
    screen() {
      this.getTableHistory();
    },
    handleSizeChange(val) {
      // console.log(`每页 ${val} 条`);
      this.page_size = val;
      this.getTableHistory();
    },
    handleCurrentChange(val) {
      // console.log(`当前页: ${val}`);
      this.page = val;
      this.getTableHistory();
    },
    onCellClicked(cell) {
      const { value, data } = cell;
      if (value == "详情") {
        this.indexData = data;
        this.$nextTick(() => {
          this.$refs["details"].isShow();
        });
      }
    },
    edit() {
      this.editOptions = this.options;
      this.$nextTick(() => {
        this.$refs["editTable"].editIsShow();
      });
    },
    editListen(item) {
      // console.log(item);
      this.actionName = item.table_name;
      this.$nextTick(() => {
        this.$refs["editTable"].editListenShow();
      });
    },
    notListen(value) {
      const data = {
        table_name: value.table_name,
      };
      const headers = {
        "Content-Type": "application/json",
      };
      console.log(data);
      axios({
        method: "post",
        url: "/bdatalog/unregister_table",
        data: data,
        headers: headers,
      }).then((res) => {
        console.log(res);
        if (res.data.no == 0) {
          this.$message({
            message: "取消监听成功",
            type: "success",
          });
        }else {
           this.$message.error('取消监听失败');
        }
      });
    },
  },
};
</script>
 
<style lang="scss" scoped>
::v-deep .select-dropdown__item:hover{
  background-color: #fff;
}
.Tab_Content {
  padding: 20px;
}
.table {
  height: calc(100vh - 200px);
}
.bgc {
  color: rgb(197, 194, 194);
  cursor: not-allowed;
}
.fifty-yuan {
  background-color: #fff;
}
.allAction:hover {
  background-color: #F5F7FA;
}
</style>