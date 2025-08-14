package bc


import "strings"

type name_space_type struct {

   namespace_max_len  int
   namespace_len      int
   namespace          [][2]string


}


func construct_namespace_manager()*name_space_type{

   var return_value name_space_type
   return_value.namespace_max_len  = 0
   return_value.namespace_len      = 0
   return &return_value

}




func (v *name_space_type)push_namespace( relationship,label string){

   temp := [2]string{relationship,label}
   if v.namespace_len < v.namespace_max_len {
       v.namespace[v.namespace_len] = temp
	   v.namespace_len += 1
   }else{
       v.namespace = append(v.namespace,temp)
	   v.namespace_len +=1
	   v.namespace_max_len +=1
	}


}


func (v *name_space_type)pop_namespace()(string,string) {
     if v.namespace_len < 1 {
	    panic("empty stack")
     }
   temp := v.namespace[v.namespace_len-1]
   v.namespace_len -= 1
   return temp[0],temp[1]
}


func (v *name_space_type)get_last() (string, string) {
   
     if v.namespace_len < 1 {
	    panic("empty stack")
     }
     temp := v.namespace[v.namespace_len-1]
	 return temp[0],temp[1]
}



func (v *Build_Configuration) convert_namespace()string{
     
       var temp_value  []string
       
       for i:=0;i<(*v.namespace).namespace_len;i++{
	      temp := (*v.namespace).namespace[i]
		  
          temp_value= append(temp_value,v.make_string_key( temp[0],temp[1] ))
	   }
       key_string := v.sep + strings.Join( temp_value, v.sep )
       return  key_string
}

func (v *Build_Configuration)  make_string_key( relationship,label string)string{

       return relationship+v.rel_sep+label+v.label_sep
}




