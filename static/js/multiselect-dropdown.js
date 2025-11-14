/* * [ĐÃ CHỈNH SỬA] 
 * Khối style này đã được tùy chỉnh lại để sử dụng các biến CSS 
 * từ styles.css (Tailwind) của bạn cho đồng bộ.
 */
var style = document.createElement('style');
style.setAttribute("id","multiselect_dropdown_styles");
style.innerHTML = `
.multiselect-dropdown {
  display: block;
  width: 100%;
  padding: 0.5rem 0.75rem;
  font-size: var(--text-sm, 0.875rem);
  min-height: 2.625rem; /* Căn chỉnh chiều cao với input */
  position: relative;

  /* Áp dụng style input của Tailwind */
  background-color: var(--color-white, #fff);
  border: 1px solid var(--color-gray-300, #D1D5DB); /* */
  border-radius: var(--radius-md, 0.375rem); /* */
  
  /* Áp dụng shadow-sm của Tailwind */
  --tw-shadow: 0 1px 3px 0 var(--tw-shadow-color, rgb(0 0 0 / 0.1)), 0 1px 2px -1px var(--tw-shadow-color, rgb(0 0 0 / 0.1));
  box-shadow: var(--tw-inset-shadow), var(--tw-inset-ring-shadow), var(--tw-ring-offset-shadow), var(--tw-ring-shadow), var(--tw-shadow);

  /* Mũi tên (giữ nguyên) */
  background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3e%3cpath fill='none' stroke='%23343a40' stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M2 5l6 6 6-6'/%3e%3c/svg%3e");
  background-repeat: no-repeat;
  background-position: right .75rem center;
  background-size: 16px 12px;
}
.multiselect-dropdown span.optext, .multiselect-dropdown span.placeholder{
  margin-right:0.25rem; 
  margin-bottom:0.25rem;
  padding: 0.25rem 0.5rem;
  border-radius: var(--radius-md, 0.375rem); /* */
  display:inline-block;
  font-size: var(--text-sm, 0.875rem);
}
.multiselect-dropdown span.optext{
  /* Style "pill" của Tailwind (gray-200) */
  background-color: var(--color-gray-200, #E5E7EB); /* */
  color: var(--color-gray-800, #1F2937); /* */
}
.multiselect-dropdown span.optext .optdel {
  float: right;
  margin: 0 -4px 1px 5px;
  font-size: 0.8em;
  margin-top: 2px;
  cursor: pointer;
  color: var(--color-gray-600, #4B5563); /* */
}
.multiselect-dropdown span.optext .optdel:hover { color: #c66;}
.multiselect-dropdown span.placeholder{
  color: var(--color-gray-500, #6B7280); /* */
}
.multiselect-dropdown-list-wrapper{
  /* Style shadow-md của Tailwind */
  --tw-shadow: 0 4px 6px -1px var(--tw-shadow-color, rgb(0 0 0 / 0.1)), 0 2px 4px -2px var(--tw-shadow-color, rgb(0 0 0 / 0.1));
  box-shadow: var(--tw-inset-shadow), var(--tw-inset-ring-shadow), var(--tw-ring-offset-shadow), var(--tw-ring-shadow), var(--tw-shadow);
  
  z-index: 100;
  padding: 0.5rem; /* p-2 */
  border-radius: var(--radius-md, 0.375rem); /* */
  border: 1px solid var(--color-gray-300, #D1D5DB); /* */
  display: none;
  margin: 0.25rem 0 0 0; /* mt-1 */
  position: absolute;
  top: 100%;
  left: 0;
  right: 0;
  background: white;
}
.multiselect-dropdown-list-wrapper .multiselect-dropdown-search{
  margin-bottom: 0.5rem; /* mb-2 */
  /* Áp dụng style input của Tailwind */
  width: 100%;
  display: block;
  padding: 0.5rem 0.75rem; /* p-2 px-3 */
  font-size: var(--text-sm, 0.875rem); /* */
  border: 1px solid var(--color-gray-300, #D1D5DB); /* */
  border-radius: var(--radius-md, 0.375rem); /* */
  box-sizing: border-box; /* Fix lỗi padding */
}
.multiselect-dropdown-list-wrapper .multiselect-dropdown-search:focus {
    border-color: var(--color-blue-500, #3B82F6); /* */
    box-shadow: 0 0 0 1px var(--color-blue-500, #3B82F6); 
    outline: none;
}
.multiselect-dropdown-list{
  padding:2px;
  height: 15rem;
  overflow-y:auto;
  overflow-x: hidden;
}
.multiselect-dropdown-list::-webkit-scrollbar {
  width: 6px;
}
.multiselect-dropdown-list::-webkit-scrollbar-thumb {
  background-color: #bec4ca;
  border-radius:3px;
}

.multiselect-dropdown-list div{
  padding: 5px;
  border-radius: 4px; /* bo góc nhẹ cho item */
}
.multiselect-dropdown-list input{
  height: 1.15em;
  width: 1.15em;
  margin-right: 0.35em;  
}
.multiselect-dropdown-list div.checked{
  /* Có thể thêm style (vd: bg-blue-50) nếu muốn */
}
.multiselect-dropdown-list div:hover{
  /* Style hover của Tailwind (gray-100) */
  background-color: var(--color-gray-100, #F3F4F6); /* */
}
.multiselect-dropdown span.maxselected {width:100%;}
.multiselect-dropdown-all-selector {
  border-bottom: 1px solid var(--color-gray-300, #D1D5DB); /* */
  font-weight: 600;
}
`;
document.head.appendChild(style);

/* * ========================================================================
 * PHẦN LOGIC GỐC CỦA THƯ VIỆN (KHÔNG THAY ĐỔI)
 * ========================================================================
 */

function MultiselectDropdown(options){
  var config={
    search:true,
    height:'15rem',
    placeholder:'select',
    txtSelected:'selected',
    txtAll:'All',
    txtRemove: 'Remove',
    txtSearch:'search',
    ...options
  };
  function newEl(tag,attrs){
    var e=document.createElement(tag);
    if(attrs!==undefined) Object.keys(attrs).forEach(k=>{
      if(k==='class') { Array.isArray(attrs[k]) ? attrs[k].forEach(o=>o!==''?e.classList.add(o):0) : (attrs[k]!==''?e.classList.add(attrs[k]):0)}
      else if(k==='style'){  
        Object.keys(attrs[k]).forEach(ks=>{
          e.style[ks]=attrs[k][ks];
        });
       }
      else if(k==='text'){attrs[k]===''?e.innerHTML='&nbsp;':e.innerText=attrs[k]}
      else e[k]=attrs[k];
    });
    return e;
  }

  
  document.querySelectorAll("select[multiple]").forEach((el,k)=>{
    
    var div=newEl('div',{class:'multiselect-dropdown',style:{width:'100%',padding:config.style?.padding??''}});
    el.style.display='none';
    el.parentNode.insertBefore(div,el.nextSibling);
    var listWrap=newEl('div',{class:'multiselect-dropdown-list-wrapper'});
    var list=newEl('div',{class:'multiselect-dropdown-list',style:{height:config.height}});
    var search=newEl('input',{class:['multiselect-dropdown-search'].concat([config.searchInput?.class??'form-control']),style:{width:'100%',display:el.attributes['multiselect-search']?.value==='true'?'block':'none'},placeholder:config.txtSearch});
    listWrap.appendChild(search);
    div.appendChild(listWrap);
    listWrap.appendChild(list);

    el.loadOptions=()=>{
      list.innerHTML='';
      
      if(el.attributes['multiselect-select-all']?.value=='true'){
        var op=newEl('div',{class:'multiselect-dropdown-all-selector'})
        var ic=newEl('input',{type:'checkbox'});
        op.appendChild(ic);
        op.appendChild(newEl('label',{text:config.txtAll}));
  
        op.addEventListener('click',()=>{
          op.classList.toggle('checked');
          op.querySelector("input").checked=!op.querySelector("input").checked;
          
          var ch=op.querySelector("input").checked;
          list.querySelectorAll(":scope > div:not(.multiselect-dropdown-all-selector)")
            .forEach(i=>{if(i.style.display!=='none'){i.querySelector("input").checked=ch; i.optEl.selected=ch}});
  
          el.dispatchEvent(new Event('change'));
        });
        ic.addEventListener('click',(ev)=>{
          ic.checked=!ic.checked;
        });
        el.addEventListener('change', (ev)=>{
          let itms=Array.from(list.querySelectorAll(":scope > div:not(.multiselect-dropdown-all-selector)")).filter(e=>e.style.display!=='none')
          let existsNotSelected=itms.find(i=>!i.querySelector("input").checked);
          if(ic.checked && existsNotSelected) ic.checked=false;
          else if(ic.checked==false && existsNotSelected===undefined) ic.checked=true;
        });
  
        list.appendChild(op);
      }

      Array.from(el.options).map(o=>{
        var op=newEl('div',{class:o.selected?'checked':'',optEl:o})
        var ic=newEl('input',{type:'checkbox',checked:o.selected});
        op.appendChild(ic);
        op.appendChild(newEl('label',{text:o.text}));

        op.addEventListener('click',()=>{
          op.classList.toggle('checked');
          op.querySelector("input").checked=!op.querySelector("input").checked;
          op.optEl.selected=!!!op.optEl.selected;
          el.dispatchEvent(new Event('change'));
        });
        ic.addEventListener('click',(ev)=>{
          ic.checked=!ic.checked;
        });
        o.listitemEl=op;
        list.appendChild(op);
      });
      div.listEl=listWrap;

      div.refresh=()=>{
        div.querySelectorAll('span.optext, span.placeholder').forEach(t=>div.removeChild(t));
        var sels=Array.from(el.selectedOptions);
        if(sels.length>(el.attributes['multiselect-max-items']?.value??5)){
          div.appendChild(newEl('span',{class:['optext','maxselected'],text:sels.length+' '+config.txtSelected}));          
        }
        else{
          sels.map(x=>{
            var c=newEl('span',{class:'optext',text:x.text, srcOption: x});
            if((el.attributes['multiselect-hide-x']?.value !== 'true'))
              c.appendChild(newEl('span',{class:'optdel',text:'☒',title:config.txtRemove, onclick:(ev)=>{c.srcOption.listitemEl.dispatchEvent(new Event('click'));div.refresh();ev.stopPropagation();}}));

            div.appendChild(c);
          });
        }
        if(0==el.selectedOptions.length) div.appendChild(newEl('span',{class:'placeholder',text:el.attributes['placeholder']?.value??config.placeholder}));
      };
      div.refresh();
    }
    el.loadOptions();
    
    search.addEventListener('input',()=>{
      list.querySelectorAll(":scope div:not(.multiselect-dropdown-all-selector)").forEach(d=>{
        var txt=d.querySelector("label").innerText.toUpperCase();
        d.style.display=txt.includes(search.value.toUpperCase())?'block':'none';
      });
    });

    div.addEventListener('click',()=>{
      div.listEl.style.display='block';
      search.focus();
      search.select();
    });
    
    document.addEventListener('click', function(event) {
      if (!div.contains(event.target)) {
        listWrap.style.display='none';
        div.refresh();
      }
    });    
  });
}