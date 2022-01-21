<template>
  <div class="edit_content">
    <el-dialog
      title="编辑表"
      :visible.sync="dialogVisible"
      width="30%"
      min-width="484px"
      :before-close="cancel"
      :close-on-click-modal="false"
    >
      <el-form
        :model="ruleForm"
        :rules="rules"
        ref="ruleForm"
        label-width="130px"
        class="demo-ruleForm"
      >
        <el-form-item label="表单：" prop="">
          <el-select v-model="ruleForm.value" placeholder="请选择">
            <el-option
              v-for="item in options"
              :key="item.table_name"
              :label="item.table_name"
              :value="item.table_name"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="字段：" prop="">
          <el-select v-model="ruleForm.value1" multiple placeholder="请选择">
            <el-option
              v-for="item in ruleForm.options2"
              :key="item.field_id"
              :label="item.field_name"
              :value="item.field_id"
            >
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="过期时间(天)：" prop="timeInput">
          <el-input
            v-model="ruleForm.timeInput"
            placeholder="请输入过期时间"
            style="width: 205px"
          ></el-input>
        </el-form-item>
        <el-form-item label="日志数量(条)：" prop="logInput">
          <el-input
            v-model="ruleForm.logInput"
            placeholder="请输入日志数量"
            style="width: 205px"
          ></el-input>
        </el-form-item>
      </el-form>

      <span slot="footer" class="dialog-footer">
        <el-button @click="cancel">取 消</el-button>
        <el-button type="primary" @click="postInsenseField('ruleForm')"
          >{{title}}</el-button
        >
      </span>
    </el-dialog>
  </div>
</template>

<script>
import axios from "axios";
export default {
  data() {
    const day = (rule, value, callback) => {
      if (!value) {
        return callback(new Error("天数不能为空"));
      } else if (isNaN(value)) {
        // console.log(typeof value);
        callback(new Error("请输入数字值"));
      } else if (value <= 0) {
        callback(new Error("请输入大于0的整数值"));
      } else {
        callback();
      }
    };
    const tiao = (rule, value, callback) => {
      if (!value) {
        return callback(new Error("条数不能为空"));
      } else if (isNaN(value)) {
        // console.log(typeof value);
        callback(new Error("请输入数字值"));
      } else if (value <= 0) {
        callback(new Error("请输入大于0的整数值"));
      } else {
        callback();
      }
    };
    return {
      dialogVisible: false,
      ruleForm: {
        value: "",
        options2: [],
        timeInput: 10,
        logInput: 10,
        value1: [],
      },
      rules: {
        timeInput: [{ required: true, validator: day }],
        logInput: [{ required: true, validator: tiao }],
      },
      options: [],
      title:"保存",
    };
  },
  props: {
    tableName: String,
    AllTable: Array,
    actionName: String
  },
  created() {},
  methods: {
    // 获取字段名
    getTableField() {
      const params = {
        table_name: this.ruleForm.value,
      };
      const headers = {
        "Content-Type": "application/json",
      };
      axios({
        method: "post",
        url: "/bdatalog/list_table_field",
        data: params,
        headers: headers,
      })
        .then((res) => {
          if (res.data.no == 0) {
            this.ruleForm.options2 = res.data.data;
          } else {
            console.log(res);
          }
        })
        .catch((err) => {
          console.log(err);
        });
    },
    editIsShow() {
      this.dialogVisible = true;
      this.options = this.AllTable;
      this.ruleForm.value = this.tableName;
      this.getTableField();
    },
    handleClose() {
      this.dialogVisible = false;
    },
    cancel() {
      this.ruleForm = {
        options2: [],
        timeInput: 10,
        logInput: 10,
      };
      this.value1 = [];
      this.dialogVisible = false;
    },
    postInsenseField(formName) {
      this.$refs[formName].validate((vaild) => {
        console.log(vaild);
        if (vaild) {
          let newArr = [];
          if(this.ruleForm.value1 != []) {
              this.ruleForm.value1.forEach((item) => {
            newArr.push(String(item));
          });
          }
          const data = {
            table_name: this.ruleForm.value,
            out_date: Number(this.ruleForm.timeInput),
            fields: newArr,
            min_log_num: Number(this.ruleForm.logInput),
          };
          const headers = {
            "Content-Type": "application/json",
          };
          if(this.title != '监听') {
            axios
            .post("/bdatalog/modify_table_policy", data, { headers: headers })
            .then((res) => {
              if (res.data.no == 0) {
                this.$message({
                  message: "修改成功",
                  type: "success",
                });
                this.dialogVisible = false;
                this.ruleForm = {
                  options2: [],
                  timeInput: 10,
                  logInput: 10,
                };
              } else {
                this.$message.error(res.data.data);
              }
            })
            .catch((err) => {
              this.$message.error(err);
              console.log(err);
            });
          } else {
            axios
            .post("/bdatalog/register_table", data, { headers: headers })
            .then((res) => {
              if (res.data.no == 0) {
                this.$message({
                  message: "修改监听成功",
                  type: "success",
                });
                this.dialogVisible = false;
                this.ruleForm = {
                  options2: [],
                  timeInput: 10,
                  logInput: 10,
                };
              } else {
                this.$message.error(res.data.data);
              }
            })
            .catch((err) => {
              this.$message.error(err);
              console.log(err);
            });
          }
        } else {
          return false;
        }
      });
    },
    editListenShow(){
      this.title = "监听";
      this.ruleForm.value = this.actionName;
      this.dialogVisible = true;
      this.getTableField()
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .el-dialog {
  min-width: 500px;
}
</style>