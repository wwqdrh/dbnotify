/*
 * @Author: your name
 * @Date: 2021-12-09 14:35:54
 * @LastEditTime: 2021-12-13 17:52:46
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \serve\src\main.js
 */
import Vue from 'vue'
import tableLog from './tableLog.vue'
import ElementUI from 'element-ui';
import 'normalize.css/normalize.css';
import 'element-ui/lib/theme-chalk/index.css';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-balham.css';


Vue.use(ElementUI);

Vue.config.productionTip = false

new Vue({
  render: h => h(tableLog),
}).$mount('#app')
